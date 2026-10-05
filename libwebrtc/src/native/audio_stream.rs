// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
    time::Instant,
};

use cxx::SharedPtr;
use parking_lot::Mutex;
use rtrb::{Consumer, Producer, PushError, RingBuffer};
use tokio_stream::Stream;
use webrtc_sys::audio_track as sys_at;

use crate::{
    audio_frame::AudioFrame, audio_stream::native::NativeAudioStreamQueueStats,
    audio_track::RtcAudioTrack,
};

pub struct NativeAudioStream {
    native_sink: SharedPtr<sys_at::ffi::NativeAudioSink>,
    audio_track: RtcAudioTrack,
    frame_queue: Arc<AudioFrameQueue>,
}

impl NativeAudioStream {
    pub fn new(
        audio_track: RtcAudioTrack,
        sample_rate: i32,
        num_channels: i32,
        queue_size_frames: Option<usize>,
    ) -> Self {
        let frame_queue = Arc::new(AudioFrameQueue::new(queue_size_frames));
        let observer = Arc::new(AudioTrackObserver { frame_queue: frame_queue.clone() });
        let native_sink = sys_at::ffi::new_native_audio_sink(
            Box::new(sys_at::AudioSinkWrapper::new(observer.clone())),
            sample_rate,
            num_channels,
        );

        let audio = unsafe { sys_at::ffi::media_to_audio(audio_track.sys_handle()) };
        audio.add_sink(&native_sink);

        Self { native_sink, audio_track, frame_queue }
    }

    pub fn track(&self) -> RtcAudioTrack {
        self.audio_track.clone()
    }

    /// Observes cumulative queue-overflow losses without resetting them.
    pub fn dropped_frames(&self) -> u64 {
        self.frame_queue.dropped_frames()
    }

    /// Observes decoded callback cadence and frame queue residence.
    pub fn queue_stats(&self) -> NativeAudioStreamQueueStats {
        self.frame_queue.stats_at(Instant::now())
    }

    pub fn close(&mut self) {
        let audio = unsafe { sys_at::ffi::media_to_audio(self.audio_track.sys_handle()) };
        audio.remove_sink(&self.native_sink);

        self.frame_queue.close();
    }
}

impl Drop for NativeAudioStream {
    fn drop(&mut self) {
        self.close();
    }
}

impl Stream for NativeAudioStream {
    type Item = AudioFrame<'static>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.frame_queue.poll_recv(cx)
    }
}

pub struct AudioTrackObserver {
    frame_queue: Arc<AudioFrameQueue>,
}

impl sys_at::AudioSink for AudioTrackObserver {
    fn on_data(&self, data: &[i16], sample_rate: i32, nb_channels: usize, nb_frames: usize) {
        let received_at = Instant::now();
        self.frame_queue.push_at(
            AudioFrame {
                data: data.to_owned().into(),
                sample_rate: sample_rate as u32,
                num_channels: nb_channels as u32,
                samples_per_channel: nb_frames as u32,
            },
            received_at,
        );
    }
}

struct AudioFrameQueue {
    kind: AudioFrameQueueKind,
    closed: AtomicBool,
    dropped_frames: AtomicU64,
    started_at: Instant,
    received_frames: AtomicU64,
    delivered_frames: AtomicU64,
    last_callback_offset_ns: AtomicU64,
    max_callback_gap_ns: AtomicU64,
    max_frame_residence_ns: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

/// Owns decoded PCM and its native callback's monotonic arrival time.
struct QueuedAudioFrame {
    frame: AudioFrame<'static>,
    received_at: Instant,
}

enum AudioFrameQueueKind {
    Bounded(BoundedAudioFrameQueue),
    Unbounded(UnboundedAudioFrameQueue),
}

struct BoundedAudioFrameQueue {
    producer: Mutex<Producer<QueuedAudioFrame>>,
    consumer: Mutex<Consumer<QueuedAudioFrame>>,
}

struct UnboundedAudioFrameQueue {
    frames: Mutex<VecDeque<QueuedAudioFrame>>,
}

impl AudioFrameQueue {
    /// Samples counters and current backlog without removing a frame.
    fn stats_at(&self, now: Instant) -> NativeAudioStreamQueueStats {
        let (queued_frames, oldest_received_at) = match &self.kind {
            AudioFrameQueueKind::Bounded(queue) => {
                let consumer = queue.consumer.lock();
                (consumer.slots(), consumer.peek().ok().map(|frame| frame.received_at))
            }
            AudioFrameQueueKind::Unbounded(queue) => {
                let frames = queue.frames.lock();
                (frames.len(), frames.front().map(|frame| frame.received_at))
            }
        };
        let last_callback = self.last_callback_offset_ns.load(Ordering::Relaxed);
        NativeAudioStreamQueueStats {
            received_frames: self.received_frames.load(Ordering::Relaxed),
            delivered_frames: self.delivered_frames.load(Ordering::Relaxed),
            dropped_frames: self.dropped_frames(),
            queued_frames,
            max_callback_gap_ms: self.max_callback_gap_ns.load(Ordering::Relaxed) / 1_000_000,
            max_frame_residence_ms: self.max_frame_residence_ns.load(Ordering::Relaxed) / 1_000_000,
            oldest_queued_frame_age_ms: oldest_received_at
                .map(|received_at| elapsed_ns(received_at, now) / 1_000_000)
                .unwrap_or_default(),
            last_callback_age_ms: (last_callback > 0).then(|| {
                elapsed_ns(self.started_at, now).saturating_sub(last_callback - 1) / 1_000_000
            }),
        }
    }

    fn new(capacity: Option<usize>) -> Self {
        let kind = match capacity.filter(|capacity| *capacity > 0) {
            Some(capacity) => {
                let (producer, consumer) = RingBuffer::new(capacity);
                AudioFrameQueueKind::Bounded(BoundedAudioFrameQueue {
                    producer: Mutex::new(producer),
                    consumer: Mutex::new(consumer),
                })
            }
            None => AudioFrameQueueKind::Unbounded(UnboundedAudioFrameQueue {
                frames: Mutex::new(VecDeque::new()),
            }),
        };

        Self {
            kind,
            closed: AtomicBool::new(false),
            dropped_frames: AtomicU64::new(0),
            started_at: Instant::now(),
            received_frames: AtomicU64::new(0),
            delivered_frames: AtomicU64::new(0),
            last_callback_offset_ns: AtomicU64::new(0),
            max_callback_gap_ns: AtomicU64::new(0),
            max_frame_residence_ns: AtomicU64::new(0),
            waker: Mutex::new(None),
        }
    }

    /// Observes cumulative queue-overflow losses without consuming a frame.
    fn dropped_frames(&self) -> u64 {
        self.dropped_frames.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    /// Captures a frame using the real clock for ordinary queue tests.
    fn push(&self, frame: AudioFrame<'static>) {
        self.push_at(frame, Instant::now());
    }

    /// Records callback arrival while preserving the existing queue policy.
    fn push_at(&self, frame: AudioFrame<'static>, received_at: Instant) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        self.received_frames.fetch_add(1, Ordering::Relaxed);
        // Reserve zero for "no callback". Concurrent observations cannot move
        // the latest callback backwards or manufacture a negative interval.
        let offset = elapsed_ns(self.started_at, received_at).saturating_add(1);
        let previous = self.last_callback_offset_ns.fetch_max(offset, Ordering::Relaxed);
        if previous > 0 && offset >= previous {
            self.max_callback_gap_ns.fetch_max(offset - previous, Ordering::Relaxed);
        }
        let frame = QueuedAudioFrame { frame, received_at };

        match &self.kind {
            AudioFrameQueueKind::Bounded(queue) => self.push_bounded(queue, frame),
            AudioFrameQueueKind::Unbounded(queue) => {
                queue.frames.lock().push_back(frame);
            }
        }

        self.wake_receiver();
    }

    fn push_bounded(&self, queue: &BoundedAudioFrameQueue, mut frame: QueuedAudioFrame) {
        loop {
            let push_result = queue.producer.lock().push(frame);
            match push_result {
                Ok(()) => return,
                Err(PushError::Full(returned_frame)) => {
                    frame = returned_frame;

                    let dropped = queue.consumer.lock().pop().is_ok();

                    if dropped {
                        self.record_drop();
                    } else {
                        return;
                    }
                }
            }
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake_receiver();

        match &self.kind {
            AudioFrameQueueKind::Bounded(queue) => {
                let mut consumer = queue.consumer.lock();
                while consumer.pop().is_ok() {}
            }
            AudioFrameQueueKind::Unbounded(queue) => {
                queue.frames.lock().clear();
            }
        }
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<AudioFrame<'static>>> {
        if let Some(frame) = self.try_pop() {
            return Poll::Ready(Some(frame));
        }

        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }

        *self.waker.lock() = Some(cx.waker().clone());

        if let Some(frame) = self.try_pop() {
            self.waker.lock().take();
            Poll::Ready(Some(frame))
        } else if self.closed.load(Ordering::Acquire) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn try_pop(&self) -> Option<AudioFrame<'static>> {
        self.try_pop_at(Instant::now())
    }

    /// Measures residence only for frames actually returned to the reader.
    fn try_pop_at(&self, now: Instant) -> Option<AudioFrame<'static>> {
        let frame = match &self.kind {
            AudioFrameQueueKind::Bounded(queue) => queue.consumer.lock().pop().ok(),
            AudioFrameQueueKind::Unbounded(queue) => queue.frames.lock().pop_front(),
        }?;
        self.delivered_frames.fetch_add(1, Ordering::Relaxed);
        self.max_frame_residence_ns
            .fetch_max(elapsed_ns(frame.received_at, now), Ordering::Relaxed);
        Some(frame.frame)
    }

    fn wake_receiver(&self) {
        let waker = self.waker.lock().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn record_drop(&self) {
        let dropped_frames = self.dropped_frames.fetch_add(1, Ordering::Relaxed) + 1;
        if dropped_frames == 1 || dropped_frames % 100 == 0 {
            log::warn!(
                "native audio stream queue overflow; dropped {} queued frames",
                dropped_frames
            );
        }
    }
}

/// Converts a monotonic interval into a bounded diagnostic counter.
fn elapsed_ns(started_at: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(started_at).as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::AudioFrameQueue;
    use crate::audio_frame::AudioFrame;

    fn test_frame(marker: i16) -> AudioFrame<'static> {
        AudioFrame {
            data: vec![marker].into(),
            sample_rate: 48_000,
            num_channels: 1,
            samples_per_channel: 1,
        }
    }

    fn pop_marker(queue: &AudioFrameQueue) -> Option<i16> {
        queue.try_pop().map(|frame| frame.data[0])
    }

    #[test]
    fn queue_stats_observe_capture_without_consuming_or_resetting_frames() {
        let _public_observation: fn(
            &crate::audio_stream::native::NativeAudioStream,
        )
            -> crate::audio_stream::native::NativeAudioStreamQueueStats =
            crate::audio_stream::native::NativeAudioStream::queue_stats;
        let queue = AudioFrameQueue::new(Some(2));
        queue.push(test_frame(1));
        queue.push(test_frame(2));
        queue.push(test_frame(3));
        let observed = queue.stats_at(std::time::Instant::now());
        assert_eq!(observed.received_frames, 3);
        assert_eq!(observed.delivered_frames, 0);
        assert_eq!(observed.dropped_frames, 1);
        assert_eq!(observed.queued_frames, 2);
        assert!(observed.last_callback_age_ms.is_some());
        assert_eq!(pop_marker(&queue), Some(2));
        assert_eq!(pop_marker(&queue), Some(3));
        let drained = queue.stats_at(std::time::Instant::now());
        assert_eq!(drained.received_frames, 3);
        assert_eq!(drained.delivered_frames, 2);
        assert_eq!(drained.dropped_frames, 1);
        assert_eq!(drained.queued_frames, 0);
    }

    #[test]
    fn queue_stats_distinguish_callback_cadence_from_reader_residence() {
        let queue = AudioFrameQueue::new(Some(3));
        let clock = queue.started_at;
        for marker in 0..3 {
            queue.push_at(test_frame(marker), clock + Duration::from_millis(marker as u64 * 10));
        }
        let pending = queue.stats_at(clock + Duration::from_millis(100));
        assert_eq!(pending.max_callback_gap_ms, 10);
        assert_eq!(pending.oldest_queued_frame_age_ms, 100);
        assert_eq!(pending.last_callback_age_ms, Some(80));
        assert_eq!(pending.max_frame_residence_ms, 0, "no delivered frame yet");
        assert_eq!(queue.try_pop_at(clock + Duration::from_millis(100)).unwrap().data[0], 0);
        let delivered = queue.stats_at(clock + Duration::from_millis(110));
        assert_eq!(delivered.max_frame_residence_ms, 100);
        assert_eq!(delivered.max_callback_gap_ms, 10);
        assert_eq!(delivered.oldest_queued_frame_age_ms, 100);
        assert_eq!(delivered.queued_frames, 2);
    }

    #[test]
    fn queue_stats_separate_source_gaps_from_promptly_consumed_frames() {
        let queue = AudioFrameQueue::new(None);
        let clock = queue.started_at;
        assert_eq!(queue.stats_at(clock).last_callback_age_ms, None);
        queue.push_at(test_frame(1), clock);
        queue.try_pop_at(clock + Duration::from_millis(2)).unwrap();
        queue.push_at(test_frame(2), clock + Duration::from_millis(120));
        queue.try_pop_at(clock + Duration::from_millis(122)).unwrap();
        let observed = queue.stats_at(clock + Duration::from_millis(125));
        assert_eq!(observed.max_callback_gap_ms, 120);
        assert_eq!(observed.max_frame_residence_ms, 2);
        assert_eq!(observed.last_callback_age_ms, Some(5));
        assert_eq!(observed.oldest_queued_frame_age_ms, 0);
        assert_eq!(observed.delivered_frames, 2);
        assert_eq!(observed.dropped_frames, 0);
        queue.close();
        queue.push_at(test_frame(3), clock + Duration::from_millis(140));
        let closed = queue.stats_at(clock + Duration::from_millis(150));
        assert_eq!(closed.received_frames, 2);
        assert_eq!(closed.max_callback_gap_ms, 120);
        assert_eq!(closed.max_frame_residence_ms, 2);
        assert_eq!(closed.last_callback_age_ms, Some(30));
    }

    #[test]
    fn bounded_queue_preserves_fifo_order_under_capacity() {
        let queue = AudioFrameQueue::new(Some(3));

        queue.push(test_frame(1));
        queue.push(test_frame(2));
        queue.push(test_frame(3));

        assert_eq!(pop_marker(&queue), Some(1));
        assert_eq!(pop_marker(&queue), Some(2));
        assert_eq!(pop_marker(&queue), Some(3));
        assert_eq!(pop_marker(&queue), None);
    }

    #[test]
    fn bounded_queue_drops_oldest_when_full() {
        let queue = AudioFrameQueue::new(Some(2));

        queue.push(test_frame(1));
        queue.push(test_frame(2));
        queue.push(test_frame(3));

        assert_eq!(queue.dropped_frames.load(Ordering::Relaxed), 1);
        assert_eq!(pop_marker(&queue), Some(2));
        assert_eq!(pop_marker(&queue), Some(3));
        assert_eq!(pop_marker(&queue), None);
    }

    #[test]
    fn capture_loss_remains_observable_after_queue_drain_and_close() {
        // Consumers need the public stream observation before accepting PCM;
        // log messages alone cannot make a successful turn loss-aware.
        let _public_observation: fn(&crate::audio_stream::native::NativeAudioStream) -> u64 =
            crate::audio_stream::native::NativeAudioStream::dropped_frames;
        let queue = AudioFrameQueue::new(Some(2));
        assert_eq!(queue.dropped_frames(), 0);
        queue.push(test_frame(1));
        queue.push(test_frame(2));
        queue.push(test_frame(3));
        assert_eq!(queue.dropped_frames(), 1);
        assert_eq!(pop_marker(&queue), Some(2));
        assert_eq!(pop_marker(&queue), Some(3));
        assert_eq!(queue.dropped_frames(), 1);
        queue.close();
        queue.push(test_frame(4));
        assert_eq!(queue.dropped_frames(), 1);
    }

    #[test]
    fn unbounded_queue_retains_all_frames() {
        let queue = AudioFrameQueue::new(None);

        for marker in 1..=4 {
            queue.push(test_frame(marker));
        }

        for marker in 1..=4 {
            assert_eq!(pop_marker(&queue), Some(marker));
        }
        assert_eq!(pop_marker(&queue), None);
        assert_eq!(queue.dropped_frames.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn close_clears_buffer_and_rejects_future_pushes() {
        let queue = AudioFrameQueue::new(Some(2));

        queue.push(test_frame(1));
        queue.close();
        queue.push(test_frame(2));

        assert_eq!(pop_marker(&queue), None);
    }
}

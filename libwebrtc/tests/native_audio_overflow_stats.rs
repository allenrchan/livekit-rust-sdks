// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use std::{borrow::Cow, time::Duration};

use libwebrtc::{
    audio_frame::AudioFrame,
    audio_source::{native::NativeAudioSource, AudioSourceOptions},
    audio_stream::native::{NativeAudioStream, NativeAudioStreamOptions},
    peer_connection_factory::{native::PeerConnectionFactoryExt, PeerConnectionFactory},
};
use tokio_stream::StreamExt;

/// Offers one complete 10-ms, distinctly marked PCM frame to the actual source.
async fn capture(source: &NativeAudioSource, marker: i16) {
    source
        .capture_frame(&AudioFrame {
            data: Cow::Owned(vec![marker; 160]),
            sample_rate: 16_000,
            num_channels: 1,
            samples_per_channel: 160,
        })
        .await
        .expect("the actual zero-buffer native source must accept a complete frame");
}

/// Reads the real sink, with a failure deadline rather than a readiness retry.
async fn read_marker(stream: &mut NativeAudioStream, marker: i16) {
    let frame = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("the native reader must make progress")
        .expect("the native stream must remain open");
    assert_eq!(
        (frame.sample_rate, frame.num_channels, frame.samples_per_channel),
        (16_000, 1, 160)
    );
    assert_eq!(frame.data.len(), 160);
    assert!(frame.data.iter().all(|sample| *sample == marker), "preserve PCM content and order");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_evicted_age_survives_delivery_and_close() {
    for delayed_reader in [false, true] {
        let factory = PeerConnectionFactory::default();
        let source = NativeAudioSource::new(AudioSourceOptions::default(), 16_000, 1, 0);
        let track = factory.create_audio_track("overflow-age", source.clone());
        let mut stream = NativeAudioStream::with_options(
            track,
            16_000,
            1,
            NativeAudioStreamOptions { queue_size_frames: Some(8) },
        );
        assert_eq!(stream.overflow_stats().last_evicted_frame_age_ms, None);
        capture(&source, 1).await;
        assert_eq!(stream.queue_stats().received_frames, 1);
        if delayed_reader {
            // Intentional measured reader delay, not fixture/readiness waiting.
            tokio::time::sleep(Duration::from_millis(125)).await;
        }
        for marker in 2..=9 {
            capture(&source, marker).await;
        }
        let observed = stream.overflow_stats();
        let queued = stream.queue_stats();
        assert_eq!(
            (queued.received_frames, queued.delivered_frames, queued.queued_frames),
            (9, 0, 8)
        );
        assert_eq!(observed.dropped_frames, 1);
        assert_eq!(observed.max_evicted_frame_age_ms, observed.last_evicted_frame_age_ms);
        assert_eq!(queued.max_frame_residence_ms, 0, "eviction is not reader delivery");
        let age = observed.last_evicted_frame_age_ms.expect("an eviction must have an age");
        if delayed_reader {
            assert!(age >= 125, "age must include time spent waiting for the reader");
        }
        println!("native overflow observation: delayed_reader={delayed_reader}, {observed:?}");
        for marker in 2..=9 {
            read_marker(&mut stream, marker).await;
        }
        let drained = stream.queue_stats();
        assert_eq!(
            (drained.received_frames, drained.delivered_frames, drained.queued_frames),
            (9, 8, 0)
        );
        assert_eq!(stream.overflow_stats(), observed);
        stream.close();
        capture(&source, 10).await;
        assert_eq!(
            stream.overflow_stats(),
            observed,
            "shutdown must neither record nor reset eviction"
        );
        let closed = stream.queue_stats();
        assert_eq!(
            (
                closed.received_frames,
                closed.delivered_frames,
                closed.dropped_frames,
                closed.queued_frames
            ),
            (9, 8, 1, 0),
            "post-close capture must not enter the sink"
        );
        assert_eq!(closed.max_callback_gap_ms, drained.max_callback_gap_ms);
        assert_eq!(closed.max_frame_residence_ms, drained.max_frame_residence_ms);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_prompt_delivery_has_no_eviction_age() {
    let factory = PeerConnectionFactory::default();
    let source = NativeAudioSource::new(AudioSourceOptions::default(), 16_000, 1, 0);
    let track = factory.create_audio_track("no-overflow-age", source.clone());
    let mut stream = NativeAudioStream::with_options(
        track,
        16_000,
        1,
        NativeAudioStreamOptions { queue_size_frames: Some(8) },
    );
    for marker in 1..=9 {
        capture(&source, marker).await;
        read_marker(&mut stream, marker).await;
    }
    let stats = stream.queue_stats();
    assert_eq!(
        (stats.received_frames, stats.delivered_frames, stats.dropped_frames, stats.queued_frames),
        (9, 9, 0, 0)
    );
    let observed = stream.overflow_stats();
    assert_eq!(observed.dropped_frames, 0);
    assert_eq!(observed.last_evicted_frame_age_ms, None);
    assert_eq!(observed.max_evicted_frame_age_ms, None);
    stream.close();
    assert_eq!(stream.overflow_stats(), observed);
}

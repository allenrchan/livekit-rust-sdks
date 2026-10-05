/*
 * Copyright 2026 LiveKit, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 */

// Drives the real SDK pump's repeating callback, without sleeping or opening
// a room. Only WebRTC's task scheduling is substituted; no pump code is copied.
#include "livekit/synthetic_audio_device.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <string>
#include <utility>

#include "api/environment/environment_factory.h"
#include "api/make_ref_counted.h"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) {
    std::fprintf(stderr, "FAIL: %s\n", message);
    std::exit(1);
  }
}

class ManualQueue;
struct QueueState {
  ManualQueue* active = nullptr;
  int deleted = 0;
};

class ManualQueue final : public webrtc::TaskQueueBase {
 public:
  explicit ManualQueue(QueueState& state) : state_(state) {}

  void Delete() override {
    {
      CurrentTaskQueueSetter current(this);
      tasks_.clear();
    }
    state_.active = nullptr;
    ++state_.deleted;
    delete this;
  }

  void Tick() {
    Require(tasks_.size() == 1, "exactly one pump callback must be scheduled");
    auto task = std::move(tasks_.front());
    tasks_.pop_front();
    CurrentTaskQueueSetter current(this);
    std::move(task)();
    Require(tasks_.size() == 1, "one pump callback must remain scheduled");
  }

 private:
  void PostTaskImpl(absl::AnyInvocable<void() &&> task,
                    const PostTaskTraits&,
                    const webrtc::Location&) override {
    tasks_.push_back(std::move(task));
  }

  void PostDelayedTaskImpl(absl::AnyInvocable<void() &&> task,
                           webrtc::TimeDelta,
                           const PostDelayedTaskTraits&,
                           const webrtc::Location&) override {
    tasks_.push_back(std::move(task));
  }

  QueueState& state_;
  std::deque<absl::AnyInvocable<void() &&>> tasks_;
};

class ManualFactory final : public webrtc::TaskQueueFactory {
 public:
  explicit ManualFactory(QueueState& state) : state_(state) {}
  std::unique_ptr<webrtc::TaskQueueBase, webrtc::TaskQueueDeleter>
  CreateTaskQueue(absl::string_view, Priority) const override {
    Require(state_.active == nullptr, "previous queue must be destroyed");
    auto* queue = new ManualQueue(state_);
    state_.active = queue;
    return std::unique_ptr<webrtc::TaskQueueBase, webrtc::TaskQueueDeleter>(queue);
  }

 private:
  QueueState& state_;
};

class Transport final : public webrtc::AudioTransport {
 public:
  int calls = 0;

  int32_t RecordedDataIsAvailable(const void*, size_t, size_t, size_t,
                                  uint32_t, uint32_t, int32_t, uint32_t,
                                  bool, uint32_t& level) override {
    level = 0;
    return 0;
  }

  int32_t NeedMorePlayData(size_t samples, size_t bytes_per_sample,
                           size_t channels, uint32_t sample_rate, void* data,
                           size_t& samples_out, int64_t* elapsed,
                           int64_t* ntp) override {
    Require(samples == 480 && bytes_per_sample == 4 && channels == 2 &&
                sample_rate == 48000,
            "registered transport must retain ten-millisecond stereo PCM");
    std::fill_n(static_cast<int16_t*>(data), samples * channels, 0);
    samples_out = samples;
    *elapsed = 0;
    *ntp = 0;
    ++calls;
    return 0;
  }

  void PullRenderData(int, int, size_t, size_t, void*, int64_t*,
                      int64_t*) override {
    Require(false, "synthetic pump must use NeedMorePlayData");
  }
};

struct Fixture {
  QueueState state;
  ManualFactory factory{state};
  webrtc::Environment env = webrtc::CreateEnvironment(&factory);
  webrtc::scoped_refptr<livekit_ffi::SyntheticAudioDevice> device =
      webrtc::make_ref_counted<livekit_ffi::SyntheticAudioDevice>(env);

  void Start() {
    Require(device->Init() == 0 && device->StartPlayout() == 0,
            "synthetic playout must start");
    Require(device->Playing(), "playout intent must remain enabled");
  }

  void Tick() {
    Require(state.active != nullptr, "initialized pump must own its queue");
    state.active->Tick();
  }
};

void NoTransport() {
  Fixture fixture;
  // nullptr is a supported unregister operation, including before first use.
  Require(fixture.device->RegisterAudioCallback(nullptr) == 0,
          "unregister before first callback");
  fixture.Start();
  fixture.Tick();
  fixture.Tick();
  Require(fixture.device->Playing(), "no transport must not cancel playout");
}

void RegistrationLifecycle() {
  Transport first;
  Transport second;
  Fixture fixture;
  fixture.Start();
  // No RegisterAudioCallback call yet: construction must initialize absence.
  fixture.Tick();
  Require(fixture.device->RegisterAudioCallback(&first) == 0, "register first");
  fixture.Tick();
  Require(first.calls == 1, "registered callback must be called once");
  Require(fixture.device->RegisterAudioCallback(nullptr) == 0, "unregister");
  fixture.Tick();
  Require(first.calls == 1, "unregistered callback must not be invoked again");
  Require(fixture.device->RegisterAudioCallback(&second) == 0, "register second");
  fixture.Tick();
  Require(first.calls == 1 && second.calls == 1, "only current transport receives PCM");
  Require(fixture.device->StopPlayout() == 0, "stop playout");
  fixture.Tick();
  Require(second.calls == 1, "stopped playout must not request PCM");
}

void RestartCleanup() {
  Transport transport;
  Fixture fixture;
  fixture.Start();
  Require(fixture.device->RegisterAudioCallback(&transport) == 0, "register");
  fixture.Tick();
  Require(fixture.device->RegisterAudioCallback(nullptr) == 0, "unregister");
  Require(fixture.device->Terminate() == 0, "terminate");
  Require(fixture.state.active == nullptr && fixture.state.deleted == 1,
          "termination must destroy the old queue and its pending callback");
  fixture.Start();
  fixture.Tick();
  Require(transport.calls == 1, "restart must not resurrect old transport");
  Require(fixture.device->RegisterAudioCallback(&transport) == 0, "register again");
  fixture.Tick();
  Require(transport.calls == 2, "restarted pump must deliver registered PCM");
  Require(fixture.device->Terminate() == 0, "terminate again");
  Require(fixture.state.active == nullptr && fixture.state.deleted == 2,
          "repeated termination must destroy both owned queues");
}

}  // namespace

int main(int argc, char** argv) {
  const std::string selection = argc == 2 ? argv[1] : "all";
  int passed = 0;
  for (auto [name, test] : {std::pair{"no_transport", NoTransport},
                            std::pair{"registration_lifecycle", RegistrationLifecycle},
                            std::pair{"restart_cleanup", RestartCleanup}}) {
    if (selection == "all" || selection == name) {
      std::printf("RUN %s\n", name);
      std::fflush(stdout);
      test();
      std::printf("PASS %s\n", name);
      ++passed;
    }
  }
  Require(passed > 0, "selection must match a native test");
  std::printf("%d native pump cases passed\n", passed);
}

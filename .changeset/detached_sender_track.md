---
libwebrtc: patch
webrtc-sys: patch
livekit: patch
livekit-api: patch
livekit-capture: patch
livekit-ffi: patch
---
Preserve optional RTP sender tracks when clearing, inspecting or replacing them instead of dereferencing null native tracks during teardown.

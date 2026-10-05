---
"webrtc-sys": patch
---

Keep synthetic playout safe before audio transport registration and after unregistration, while preserving delivery to the current registered transport and deterministic queue teardown.

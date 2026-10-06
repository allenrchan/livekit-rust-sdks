---
libwebrtc: patch
---

Retry bounded native audio insertion after acquiring consumer ownership so concurrent readers do not cause unnecessary eviction or silently discard the incoming frame.

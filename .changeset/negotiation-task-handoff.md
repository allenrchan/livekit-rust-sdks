---
livekit: patch
---

Prevent a completed publisher negotiation from stranding the next media negotiation request by keeping queue state and task ownership under the same lock.

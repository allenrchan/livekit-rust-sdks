---
livekit: patch
livekit-signaling: patch
---

Keep native peer construction off the async executor with bounded owned preparation and explicit cleanup before session adoption, and settle signaling stream tasks when abandoned session setup drops its events receiver.

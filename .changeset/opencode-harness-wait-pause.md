---
"agents": patch
---

`OpenCodeHarness`: `wait()` and `prompt()` return as soon as the run ends. They paused 1 second after every run, a pause meant only for a run resuming after an eviction.

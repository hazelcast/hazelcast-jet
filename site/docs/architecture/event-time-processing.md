---
title: Event Time-Based Processing
description: Event-time related concepts in Jet (event disorder, watermarks and the likes).
---

In [Concepts](/docs/concepts/event-time) we introduced event time and,
especially, event disorder. Here we'll present some of the tricks Jet
uses to deal with it.

The key event-time concept is the *watermark* which corresponds to the
notion of *now* in the processing-time domain. It marks a position in
the data stream and tells what time it is at that position.

---
title: Sliding Window Aggregation
description: How Jet optimizes the computation of a sliding window
---

Computing a dynamic quantity like "gretchen83's current speed and
direction" requires us to aggregate data over some time period. This is
what makes the sliding window so important: it tracks the value of such
a quantity in real time.

Hazelcast Jet pays special attention to reducing the computational cost
of the sliding window. Let's present the challenges.

If we start from the general concept of windowing, this is the basic
picture: there is a stream of timestamped data items (_events_) and a
set of time intervals called _windows_. They can overlap, so any given
event may belong to more than one window. We want to apply an aggregate
function to the data of each window. Let's use counting as a simple
example:

![Time Windowing](assets/arch-sliding-window-1.svg)

To compute the output for a window, you must run every item in it
through the aggregation algorithm, and you must do this computation for
each window independently. Theoretically, you must retain all the events
until the window has "closed" (event time has advanced beyond the window
end) and then then pass them to the aggregation function in the correct
time order. However, many important aggregate functions can be
decomposed into a function you can apply in any order (in math terms, a
_commutative_ function) and another one that you apply to the result of
the first one after all seeing all the events.




Taken to the case of the sliding window, the set of windows consists of
fixed-size, overlapping windows, their start positions separated by a
fixed period:




Calculating a single sliding window result can be quite computationally
intensive, but we also expect it to slide smoothly and give a new result
often, even many times per second. This is why we gave special attention
to optimizing this computation.

We optimize especially heavily for those aggregate operations that have
a cheap way of combining partial results and even more so for those
which can cheaply undo the combining. For cheap combining you have to
express your operation in terms of a commutative and associative (CA for
short) function; to undo a combine you need the notion of "`negating`" an
argument to the function. A great many operations can be expressed
through CA functions: average, variance, standard deviation and linear
regression are some examples. All of these also support the undoing
(which we call _deduct_). The computation of extreme values (min/max) is
an example that has CA, but no good notion of negation and thus doesn't
support deducting.

This is the way we leverage the above properties: our sliding window
actually "`hops`" in fixed-size steps. The length of the window is an
integer multiple of the step size. Under such a definition, the
_tumbling_ window becomes just a special case with one step per window.

This allows us to divide the timestamp axis into _frames_ of equal
length and assign each event to its frame. Instead of keeping the event
object, we immediately pass it to the aggregate operation's _accumulate_
primitive. To compute a sliding window, we take all the frames covered
by it and combine them. Finally, to compute the next window, we just
_deduct_ the trailing frame and _combine_ the leading frame into the
existing result.

Even without _deduct_ the above process is much cheaper than the most
naïve approach where you'd keep all data and recompute everything from
scratch each time. After accumulating an item just once, the rest of the
process has fixed cost regardless of input size. With _deduct_, the
fixed cost approaches zero.

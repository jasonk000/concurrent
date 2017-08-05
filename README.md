# Concurrent utilities

PRs welcome, these are collections of utilities I have written or wished was available
when attempting to build a faster and more concurrent system.

Please read the source before applying them to your system as they will kick off
new threads with waiting loops etc. You might need to tune them for your solution.

## AsyncOutputStreamQueue

Allows offload of the work to the next stage of an OutputStream on another thread
via a queue. This means your producer can keep generating data to put on the output
stream while the next stage is performing the actual output work.

Ideally this is what the PipedInputStream / PipedOutputStream should do.  Unfortunately
the PipedOutputStream has 1sec sleeps and requires wait/notify or other solutions
to get a reasonable performance.

## BufferedOutputStream

A BufferedOutputStream drop-in replacement that uses atomic CAS instead of synchronized
blocks to get data through. If you have many threads attempting to write to a fast
OutputStream, then this stream should operate a lot faster.

As an example, both other OutputStreams here queue the work on other threads, and tend
to return very quickly, this means the overhead of a synchronized call can be quite large
with lots of writers

## BufferedOutputStream2

Another attempt, using an MPSC queue and a thread. In my testing this was a little slower
than the CAS solution, so you should probably prefer that. Left here for testing and/or
further improvements.

## ParallelGZIPOutputStream

A parallel implementation of gzip for the JVM. A drop-in replacement for the existing
GZIPOutputStream. Achieve a significant improvement in your gzip output performance.

Inspired by Shevek's work, only without a buffer and with a little less locking
https://github.com/shevek/parallelgzip

## CustomBlockingMpmcQueue

When Java's out of the box ArrayBlockingQueue and similar become a bit slow, you can
switch to NitsanW's JCTools queues. Unfortunately they don't out of the box implement
BlockingQueue, which you need for an Executor. Use this adapter in your executor and
you can take advantage of the JCTools queue performance in your executor.


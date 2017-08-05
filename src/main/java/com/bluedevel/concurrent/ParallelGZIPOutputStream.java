package com.bluedevel.concurrent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * A parallel implementation of a gzip output stream.
 *
 * A parallel implementation of gzip for the JVM. A drop-in replacement for the existing
 * GZIPOutputStream. Achieve a significant improvement in your gzip output performance.
 *
 * Inspired by Shevek's work, only without a buffer and with a little less locking
 * https://github.com/shevek/parallelgzip
 *
 * Works in chunks and gzips each them before passing them to the next
 * OutputStream.
 *
 * For efficiency, you should pass larger chunks to the stream to ensure
 * the stream has a chance to compress effectively. No buffering is
 * performed inside this class.
 *
 * Note this uses Thread.sleep(1) as a "backoff" mechanism so it's intended
 * to be used quickly and close() as soon as possible.
 */
public class ParallelGZIPOutputStream extends OutputStream {

    /**
     * Implementation notes
     *
     * Caller (upstream) of this class should call write() with appropriately sized chunks.
     * Compression tasks are submitted to an executor and placed in a queue for processing.
     * There's a chance that some compression tasks will be faster than others, so we use
     * a FIFO queue to track the tasks and ensure they are handled by the writer in the
     * correct order.
     *
     * CompressionTask task simply compresses a single chunk and passes the output to downstream
     * queue.
     *
     * Writer thread pulls from queue, waiting if needed to ensure chunks are in order,
     * and then writes the actual data to the output stream.
     *
     * Flush and close are handled by placing a special buffer into the queue. Caller thread
     * must ensure a buffer is finalised and sent through to ensure downstream is flushed
     * and closed.
     */

    private final ConcurrentLinkedQueue<Future<OutputBuffer>> writerQueue;
    private final Writer writer;
    private final ThreadPoolExecutor threadPool;
    private volatile boolean isClosed = false;

    public ParallelGZIPOutputStream(final OutputStream out, final ThreadPoolExecutor tpe) throws IOException {
        this.writerQueue = new ConcurrentLinkedQueue<Future<OutputBuffer>>();
        this.threadPool = tpe;
        this.writer = new Writer(writerQueue, out);
        threadPool.submit(writer);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        isClosed = true;
        writerQueue.add(threadPool.submit(() -> OutputBuffer.close()));
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        writerQueue.add(threadPool.submit(() -> OutputBuffer.flush()));
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        if (len == 0) return;

        byte[] local = b.clone();
        writerQueue.add(threadPool.submit(new CompressionTask(new InputBuffer(local, off, len))));
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }

    /**
     * Wraps up input data before compression
     */
    private static class InputBuffer {
        private final byte[] data;
        private final int offset;
        private final int length;
        public InputBuffer(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }
    }

    /**
     * Wraps up a data buffer after compression
     */
    private static class OutputBuffer {
        private final byte[] data;
        private final int offset;
        private final int length;
        private final boolean flush;
        private final boolean close;
        private final InputBuffer parent;
        public OutputBuffer(byte[] data, int offset, int length, InputBuffer parent, boolean flush, boolean close) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.parent = parent;
            this.flush = flush;
            this.close = close;
        }

        public OutputBuffer(byte[] data, int offset, int length, InputBuffer parent) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.parent = parent;
            this.flush = false;
            this.close = false;
        }

        public static OutputBuffer flush() {
            return new OutputBuffer(null, 0, 0, null, true, false);
        }

        public static OutputBuffer close() {
            return new OutputBuffer(null, 0, 0, null, false, true);
        }

    }

    /**
     * Takes a buffer, compresses it, stores it in another buffer, and returns the result
     */
    private static class CompressionTask implements Callable<OutputBuffer> {

        private static class CompressionTaskState {
            private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(1024);
            private final Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
            private final DeflaterOutputStream deflaterStream = new DeflaterOutputStream(byteBuffer, deflater, 1024, true);
        }

        private static final ThreadLocal<CompressionTaskState> compressorState = new ThreadLocal<CompressionTaskState>() {
            protected CompressionTaskState initialValue() {
                return new CompressionTaskState();
            }
        };

        private final InputBuffer buffer;
        public CompressionTask(InputBuffer buffer) {
            this.buffer = buffer;
        }

        public OutputBuffer call() throws IOException {
            CompressionTaskState state = compressorState.get();
                
            state.deflater.reset();
            state.byteBuffer.reset();

            state.deflaterStream.write(buffer.data, buffer.offset, buffer.length);
            state.deflaterStream.flush();
            byte[] bytes = state.byteBuffer.toByteArray();

            return new OutputBuffer(bytes, 0, bytes.length, buffer);
        }
    }

    /**
     * Processes all buffers coming in from the queue and writes them to the output destination.
     */
    private class Writer implements Runnable {
        private static final int GZIP_MAGIC = 0x8b1f;

        private final OutputStream out;
        private final ConcurrentLinkedQueue<Future<OutputBuffer>> queue;
        private long nextToWrite = 0;
        private boolean allDone = false;
        private long totalBytes = 0;
        private final CRC32 crc32 = new CRC32();

        public Writer(final ConcurrentLinkedQueue<Future<OutputBuffer>> queue, final OutputStream out) throws IOException {
            this.queue = queue;
            this.out = out;
            writeGzipHeader();
        }

        private void writeGzipHeader() throws IOException {
            out.write(new byte[] {
                (byte) GZIP_MAGIC,
                (byte) (GZIP_MAGIC >> 8),
                Deflater.DEFLATED,
                0,
                0, 0, 0, 0,
                0, 3 });
        }

        public void run() {
            try {
                while (!allDone) {
                    Future<OutputBuffer> next = queue.poll();
                    if (next == null) {
                        Thread.sleep(1);
                    } else {
                        handleBuffer(next.get());
                    }
                }
            } catch (ExecutionException | InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

        private void handleBuffer(OutputBuffer b) throws IOException {
            if (b.data != null) {
                out.write(b.data, b.offset, b.length);
                totalBytes += b.parent.length;
                crc32.update(b.parent.data, b.parent.offset, b.parent.length);
            } else if (b.flush) {
                out.flush();
            } else if (b.close) {
                writeGzipTrailer();
                out.flush();
                out.close();
                allDone = true;
            }
        }

        private void writeGzipTrailer() throws IOException {
            new DeflaterOutputStream(out, new Deflater(Deflater.DEFAULT_COMPRESSION, true), 512, true).finish();

            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt((int) crc32.getValue());
            buf.putInt((int) (totalBytes % 4294967296L));

            out.write(buf.array());
        }
    }
}


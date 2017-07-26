package com.bluedevel.util;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * A parallel implementation of a gzip output stream.
 *
 * Works in chunks and gzips each them before passing them to the next
 * OutputStream.
 *
 * For efficiency, you should pass larger chunks to the stream to ensure
 * the stream has a chance to compress effectively. No buffering is
 * performed inside this class.
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

    private final ConcurrentLinkedQueue<Future<Buffer>> writerQueue;
    private final Writer writer;
    private final ThreadPoolExecutor threadPool;
    private volatile boolean isClosed = false;

    public ParallelGZIPOutputStream(final OutputStream out, final ThreadPoolExecutor tpe) throws IOException {
        this.writerQueue = new ConcurrentLinkedQueue<Future<Buffer>>();
        this.threadPool = tpe;
        this.writer = new Writer(writerQueue, out);
        threadPool.submit(writer);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        isClosed = true;
        writerQueue.add(threadPool.submit(() -> Buffer.close()));
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        writerQueue.add(threadPool.submit(() -> Buffer.flush()));
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (isClosed) throw new IOException("stream already closed");
        byte[] local = b.clone();
        writerQueue.add(threadPool.submit(new CompressionTask(Buffer.data(local, off, len))));
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }

    /**
     * Wraps up a data buffer for handover between the different stages
     */
    private static class Buffer {
        private final byte[] data;
        private final int offset;
        private final int length;
        private final boolean flush;
        private final boolean close;
        private final Buffer parent;
        public Buffer(byte[] data, int offset, int length, boolean flush, boolean close, Buffer parent) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.flush = flush;
            this.close = close;
            this.parent = parent;
        }

        public static Buffer data(byte[] data, int offset, int length) {
            return new Buffer(data, offset, length, false, false, null);
        }

        public static Buffer data(byte[] data, int offset, int length, Buffer parent) {
            return new Buffer(data, offset, length, false, false, parent);
        }

        public static Buffer flush() {
            return new Buffer(null, 0, 0, true, false, null);
        }

        public static Buffer close() {
            return new Buffer(null, 0, 0, false, true, null);
        }

    }

    /**
     * Takes a buffer, compresses it, stores it in another buffer, and returns the result
     */
    private static class CompressionTask implements Callable<Buffer> {

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

        private final Buffer buffer;
        public CompressionTask(Buffer buffer) {
            this.buffer = buffer;
        }

        public Buffer call() throws IOException {
            CompressionTaskState state = compressorState.get();
                
            state.deflater.reset();
            state.byteBuffer.reset();

            state.deflaterStream.write(buffer.data, buffer.offset, buffer.length);
            state.deflaterStream.flush();
            byte[] bytes = state.byteBuffer.toByteArray();

            return Buffer.data(bytes, 0, bytes.length, buffer);
        }
    }

    /**
     * Processes all buffers coming in from the queue and writes them to the output destination.
     */
    private class Writer implements Runnable {
        private static final int GZIP_MAGIC = 0x8b1f;

        private final OutputStream out;
        private final ConcurrentLinkedQueue<Future<Buffer>> queue;
        private long nextToWrite = 0;
        private boolean allDone = false;
        private long totalBytes = 0;
        private final CRC32 crc32 = new CRC32();

        public Writer(final ConcurrentLinkedQueue<Future<Buffer>> queue, final OutputStream out) throws IOException {
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
                    Future<Buffer> next = queue.poll();
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

        private void handleBuffer(Buffer b) throws IOException {
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


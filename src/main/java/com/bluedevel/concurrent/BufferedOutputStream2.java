package com.bluedevel.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.jctools.queues.MpscArrayQueue;

/**
 * A BufferedOutputStream drop-in replacement that uses an MPSC queue.
 *
 * Probably prefer the other CAS based stream as it is faster.
 */
public class BufferedOutputStream2 extends OutputStream {

    /**
     * Implementation notes:
     *
     * Values come in on a write() call.
     *
     * Buffers are not actually copied. A 'chunk' is wrapped
     * around a byte array, and that chunk is then placed in
     * a queue. A thread is created to read from the MPSC
     * queue and push to the destination stream.
     */

    private class Chunk {
        public byte[] bytes;
        public int offset;
        public int length;

        public Chunk(byte[] bytes, int offset, int length) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }
    }

    private class Consumer implements Runnable {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(24576);

        @Override
        public void run() {
            try {
                while(true) {
                    Chunk chunk = queue.poll();

                    if (chunk == null) {
                        Thread.sleep(1);
                        continue;
                    }

                    if (chunk.offset == -1) {
                        buffer.writeTo(out);
                        out.flush();
                    }

                    if (chunk.offset == -2) {
                        buffer.writeTo(out);
                        out.flush();
                        out.close();
                        break;
                    }

                    buffer.write(chunk.bytes, chunk.offset, chunk.length);

                    if (buffer.size() >= 24576) {
                        buffer.writeTo(out);
                        buffer.reset();
                    }
                }
                hasFinished = true;
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private volatile boolean isClosed;
    private volatile boolean hasFinished;
    private final OutputStream out;
    private final MpscArrayQueue<Chunk> queue;
    private final Chunk POISON_PILL = new Chunk(null, -2, -2);
    private final Chunk FLUSH = new Chunk(null, -1, -1);
    public BufferedOutputStream2(final OutputStream out) {
        this.isClosed = false;
        this.out = out;
        this.queue = new MpscArrayQueue<>(8192);
        new Thread(new Consumer()).start();
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        if (isClosed) throw new IOException("stream is closed");

        Chunk chunk = new Chunk(b, offset, length);

        while(!queue.offer(chunk)) {
            // busy spin
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (isClosed) throw new IOException("stream is closed");
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        if (isClosed) throw new IOException("stream is closed");
        write(new byte[] { (byte) (b & 0xff) });
    }

    @Override
    public void close() throws IOException {
        if (isClosed) throw new IOException("stream is closed");
        isClosed = true;

        while(!queue.offer(POISON_PILL)) {
            Thread.yield();
        }

        while(!hasFinished) {
            Thread.yield();
        }
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) throw new IOException("stream is closed");

        while(!queue.offer(FLUSH)) {
            Thread.yield();
        }
    }

}

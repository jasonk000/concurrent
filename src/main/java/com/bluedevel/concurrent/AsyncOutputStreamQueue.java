package com.bluedevel.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Allows offload of the work to the next stage of an OutputStream on another thread
 * via a queue. This means your producer can keep generating data to put on the output
 * stream while the next stage is performing the actual output work.
 * 
 * Ideally this is what the PipedInputStream / PipedOutputStream should do.  Unfortunately
 * the PipedOutputStream has 1sec sleeps and requires wait/notify or other solutions
 * to get a reasonable performance.
 */
public class AsyncOutputStreamQueue extends OutputStream {

    /**
     * Implementation notes:
     *
     * A byte array of length 0 is used to flag the "close" or end of stream.
     * So, we need to make sure we do not pass a zero-length array to the queue
     * and ensure it is only sent if close() is explicitly called
     */

    private static final int BUFFER_SIZE = 64;
    private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    public AsyncOutputStreamQueue(final OutputStream out) {
        new Thread(new QueueReader(out)).start();
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b.length == 0) return;

        try {
            queue.put(b);
        } catch (InterruptedException ignored) {
            throw new IOException("was interrupted");
        }
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        if (length == 0) return;
        
        byte[] dest = new byte[length];
        System.arraycopy(b, offset, dest, 0, length);
        write(dest);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] { (byte) (b & 0xff) });
    }

    @Override
    public void close() throws IOException {
        try {
            queue.put(new byte[0]);
        } catch (InterruptedException ignored) {
            throw new IOException("was interrupted");
        }
    }

    @Override
    public void flush() {
        // TODO would be nice to handle this
    }

    private class QueueReader implements Runnable {
        private final OutputStream out;
        public QueueReader(final OutputStream out) {
            this.out = out;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ArrayList<byte[]> buffers = new ArrayList<>(BUFFER_SIZE * 2);
                    buffers.add(queue.take());
                    queue.drainTo(buffers);
                    for(byte[] buffer : buffers) {
                        if (buffer.length == 0) {
                            return;
                        }
                        out.write(buffer);
                    }
                    buffers.clear();
                }
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                close(out);
            }
        }

        private void close(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}

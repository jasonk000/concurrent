package com.iflix.cdslogprocessor.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class AsyncOutputStreamQueue extends OutputStream {

    private static final int BUFFER_SIZE = 64;
    private final ArrayBlockingQueue<BufferHolder> queue = new ArrayBlockingQueue<BufferHolder>(BUFFER_SIZE);
    public AsyncOutputStreamQueue(final OutputStream out, final String threadName) {
        new Thread(new QueueReader(out), threadName).start();
    }

    /**
     * A byte array of length 0 is used to flag the "close" or end of stream.
     * So, we need to make sure we do not pass a zero-length array to the queue
     * and ensure it is only sent if close() is explicitly called
     */
    @Override
    public void write(byte[] b) {
        if (b.length != 0) {
            try {
                queue.put(new BufferHolder(b));
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public void write(byte[] b, int offset, int length) {
        if (length != 0) {
            byte[] dest = new byte[length];
            System.arraycopy(b, offset, dest, 0, length);
            write(dest);
        }
    }

    @Override
    public void write(int b) {
        write(new byte[] { (byte) (b & 0xff) });
    }

    @Override
    public void close() {
        try {
            queue.put(new BufferHolder(new byte[0]));
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void flush() { }

    private class BufferHolder {
        public final byte[] buffer;
        public BufferHolder(final byte[] buffer) {
            this.buffer = buffer;
        }
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
                    ArrayList<BufferHolder> buffers = new ArrayList<BufferHolder>(BUFFER_SIZE * 2);
                    buffers.add(queue.take());
                    queue.drainTo(buffers);
                    for(BufferHolder bufferHolder : buffers) {
                        if (bufferHolder.buffer.length == 0) {
                            out.flush();
                            return;
                        }
                        out.write(bufferHolder.buffer);
                    }
                    buffers.clear();
                    out.flush();
                }
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
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

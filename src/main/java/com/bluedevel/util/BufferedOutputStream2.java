package com.bluedevel.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This class provides a buffered outputstream that works better under contention
 * than the JDK default BufferedOutputStream.
 */
public class BufferedOutputStream2 extends OutputStream {

    /**
     * Implementation notes:
     *
     * Values come in on a write() call.
     *
     * State is tracked with a (buffer, start) object, which tracks
     * where we currently are. An AtomicReference CAS is used to
     * claim space (once a reference owns the CAS slots, it writes
     * to the slots). At any point that a thread wants to replace the
     * buffer it can CAS out the buffer as well. If so, the thread that
     * replaces the buffer must do the write to the downstream.
     *
     * To ensure that all threads who should have written to the
     * blocks are complete, there is an additional CAS on an AtomicInteger
     * to specify the total write length.
     */

    private static final int BUFFER_SIZE = 24576;

    private static class State {
        public final byte[] buffer;
        public final int claimed;
        public final int written;

        public State() {
            this.buffer = new byte[BUFFER_SIZE];
            this.claimed = 0;
            this.written = 0;
        }

        public State(final int claimed) {
            this.buffer = new byte[BUFFER_SIZE];
            this.claimed = claimed;
            this.written = 0;
        }

        public State(final byte[] buffer, final int claimed, final int written) {
            this.buffer = buffer;
            this.claimed = claimed;
            this.written = written;
        }

        public boolean canClaim(int claim) {
            return (buffer.length > (this.claimed + claim));
        }

        public State claim(int claim) {
            return new State(this.buffer, this.claimed + claim, this.written);
        }

        public State written(int written) {
            return new State(this.buffer, this.claimed, this.written + written);
        }
    }

    private final AtomicReference<State> stateRef;
    private volatile boolean isClosed;
    private final OutputStream out;
    private final int size;
    public BufferedOutputStream2(final OutputStream out, int size) {
        this.out = out;
        this.size = size;
        this.stateRef = new AtomicReference<State>(new State());
        this.isClosed = false;
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        if (isClosed) throw new IOException("stream is closed");
        
        State ownedState = null;
        while(true) {
            State current = stateRef.get();

            // if state can accept more bytes, then try to CAS in a claim
            if (current.canClaim(length)) {
                State newState = current.claim(length);
                boolean didClaim = stateRef.compareAndSet(current, newState);
                if (didClaim) {
                    ownedState = newState;
                    break;
                }

            // if state cannot accept more bytes, then try to CAS in a new array and claim
            } else {
                flush();
            } 
        }

        // write bytes to buffer
        System.arraycopy(b, offset, ownedState.buffer, ownedState.claimed - length, length);

        // CAS update that we have written bytes
        while(true) {
            State current = stateRef.get();
            State newState = current.written(length);
            boolean didUpdate = stateRef.compareAndSet(current, newState);
            if (didUpdate) break;
        }

        // all done now
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

        // wait for writes to catch up
        while(true) {
            State current = stateRef.get();
            if (current.written == current.claimed) {
                break;
            }
        }

        // close the stream
        out.close();
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) throw new IOException("stream is closed");

        while(true) {
            State current = stateRef.get();

            // wait for writes to catch up
            if (current.written != current.claimed) {
                continue;
            }

            State newState = new State();

            boolean didUpdate = stateRef.compareAndSet(current, newState);

            // if we managed to replace the buffer, do the actual flushing
            if (didUpdate) {
                out.write(current.buffer, 0, current.written);
                out.flush();
                return;
            }
        }
    }

}

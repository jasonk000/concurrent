package com.bluedevel.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A BufferedOutputStream drop-in replacement that uses atomic CAS instead of synchronized
 * blocks to get data through. If you have many threads attempting to write to a fast 
 * OutputStream, then this stream will operate a lot faster than the standard.
 */
public class BufferedOutputStream extends OutputStream {

    /**
     * Implementation notes:
     *
     * Values come in on a write() call.
     *
     * State is tracked with a State object, which tracks
     * where we currently. An AtomicReference CAS is used to
     * claim space (once a reference owns the buffer slots, it writes
     * to the slots). At any point that a thread wants to replace the
     * buffer it can CAS out the buffer as well. If so, the thread that
     * replaces the buffer must do the write to the downstream.
     *
     * To ensure that all threads who should have published to the
     * blocks are complete, there is an additional CAS on an AtomicInteger
     * to specify the total write length.
     *
     * TODO on large core count machines the flush() spin becomes a bottleneck
     * TODO on write() call with lengths larger than buffer_size is not handled
     */

    private static final int BUFFER_SIZE = 24576;
    private static final int STRIPE_COUNT = 32;
    private static final int STRIPE_MASK = (STRIPE_COUNT - 1);

    private static class State {
        public final byte[] buffer;
        public final int claimed;
        public final int published;

        public State() {
            this.buffer = new byte[BUFFER_SIZE];
            this.claimed = 0;
            this.published = 0;
        }

        public State(final int claimed) {
            this.buffer = new byte[BUFFER_SIZE];
            this.claimed = claimed;
            this.published = 0;
        }

        public State(final byte[] buffer, final int claimed, final int published) {
            this.buffer = buffer;
            this.claimed = claimed;
            this.published = published;
        }

        public boolean canClaim(int claim) {
            return (buffer.length > (this.claimed + claim));
        }

        public State claim(int claim) {
            return new State(this.buffer, this.claimed + claim, this.published);
        }

        public State publish(int published) {
            return new State(this.buffer, this.claimed, this.published + published);
        }
    }

    private final AtomicReferenceArray<State> stateRefs;
    private volatile boolean isClosed;
    private final OutputStream out;
    public BufferedOutputStream(final OutputStream out) {
        this.out = out;
        this.stateRefs = new AtomicReferenceArray<>(STRIPE_COUNT);
        for(int i = 0; i < STRIPE_COUNT; i++) {
            stateRefs.set(i, new State());
        }
        this.isClosed = false;
    }

    private int getStripe() {
        return (int) (Thread.currentThread().getId() & STRIPE_MASK);
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        if (isClosed) throw new IOException("stream is closed");

        final int stripe = getStripe();

        State ownedState = null;
        while(true) {
            State current = stateRefs.get(stripe);

            // if state can accept more bytes, then try to CAS in a claim
            if (current.canClaim(length)) {
                State newState = current.claim(length);
                boolean didClaim = stateRefs.compareAndSet(stripe, current, newState);
                if (didClaim) {
                    ownedState = newState;
                    break;
                }
            // if state cannot accept more bytes, then flush to CAS in a new array
            } else {
                flush(stripe);
            }
        }

        // write bytes to buffer
        System.arraycopy(b, offset, ownedState.buffer, ownedState.claimed - length, length);

        // CAS update that we have published bytes
        while(true) {
            State current = stateRefs.get(stripe);
            State newState = current.publish(length);
            boolean didUpdate = stateRefs.compareAndSet(stripe, current, newState);
            if (didUpdate) break;
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] { (byte) (b & 0xff) });
    }

    @Override
    public void close() throws IOException {
        if (isClosed) throw new IOException("stream is closed");
        isClosed = true;

        int stripe = getStripe();

        for(int i = 0; i < STRIPE_COUNT; i++) {
            while(true) {
                State current = stateRefs.get(i);

                // spin to wait for writes to catch up
                if (current.published != current.claimed) {
                    continue;
                }

                break;
            }
        }

        out.close();
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) throw new IOException("stream is closed");

        for(int i = 0; i < STRIPE_COUNT; i++) {
            flush(i);
        }
    }


    private void flush(int stripe) throws IOException {
        byte[] oldbuff = null;
        while(true) {
            State current = stateRefs.get(stripe);

            if (oldbuff == null) {
                // first time through
                oldbuff = current.buffer;
            }

            if (current.claimed == 0) {
                // nothing to flush
                break;
            }

            if (oldbuff != current.buffer) {
                // someone else managed to change the buffer already
                break;
            }

            // spin to wait for writes to catch up
            if (current.published != current.claimed) {
                continue;
            }

            State newState = new State();

            boolean didUpdate = stateRefs.compareAndSet(stripe, current, newState);

            // if we managed to replace the buffer, do the actual flushing
            // to downstream
            if (didUpdate) {
                out.write(current.buffer, 0, current.published);
                out.flush();
                return;
            }
        }
    }
}

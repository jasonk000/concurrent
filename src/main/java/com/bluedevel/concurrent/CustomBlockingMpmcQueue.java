package com.bluedevel.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Collection;

import org.jctools.queues.MpmcArrayQueue;

/*
 * A BlockingQueue implementation built upon Nitsan W's JCTools queues.
 *
 * Use it in a contended executor service, not much else is supported.
 */
public class CustomBlockingMpmcQueue<E> extends MpmcArrayQueue<E> implements BlockingQueue<E> {

    public CustomBlockingMpmcQueue(int capacity) {
        super(capacity);
    }

    public void put(E e) throws InterruptedException {
        while(!offer(e)) {
            Thread.sleep(10);
        }
    }

    public E take() throws InterruptedException {
        while(true) {
            E e = poll();
            
            if (e != null) return e;

            Thread.sleep(10);
        }
    }

    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException("not implemented");
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException("not implemented");
    }

    public boolean offer(E e, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("not implemented");
    }

    public E poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("not implemented");
    }

    public int remainingCapacity() {
        throw new UnsupportedOperationException("not implemented");
    }

}

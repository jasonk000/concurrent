package com.bluedevel.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class CustomBlockingMpmcQueueTest {

    @Test
    public void testProduceOneFirstThenConsume() throws Exception {
        // producer should finish immediately, consumer should get message
        CustomBlockingMpmcQueue q = new CustomBlockingMpmcQueue(100);
        Object e = new Object();
        q.put(e);

        Assert.assertEquals(e, q.take());
        Assert.assertNull(q.peek());
    }

    @Test
    public void testConsumeFirstThenProduceOne() throws Exception {
        // consumer should block until such time as the producer has placed a message
        CustomBlockingMpmcQueue q = new CustomBlockingMpmcQueue(100);
        Object e = new Object();

        new Thread(() -> {
            try {
                Thread.sleep(100);
                q.put(e);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }).start();

        Assert.assertEquals(e, q.take());
        Assert.assertNull(q.peek());
    }

    @Test
    public void testConsumeFirstThenProduce100() throws Exception {
        // consumer should block until the producer has placed a message, then get the rest
        CustomBlockingMpmcQueue q = new CustomBlockingMpmcQueue(200);

        Object[] objs = new Object[100];
        for(int i = 0; i < objs.length; i++)
            objs[i] = new Object();

        new Thread(() -> {
            try {
                Thread.sleep(100);
                for(int i = 0; i < objs.length; i++)
                    q.put(objs[i]);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }).start();

        for(int i = 0; i < objs.length; i++)
            Assert.assertEquals(objs[i], q.take());

        Assert.assertNull(q.peek());
    }

    @Test
    public void testProduceTooManyAndConsumeSlowly() throws Exception {
        // with a capacity of 8 produce 20 and have them consumed with 0.2sec delay
        // producer should block (total delays would be 8-20=12 queued * 0.2s = 2.4sec)
        CustomBlockingMpmcQueue q = new CustomBlockingMpmcQueue(8);

        Object[] objs = new Object[20];
        for(int i = 0; i < objs.length; i++)
            objs[i] = new Object();

        final AtomicLong putFinishedMillis = new AtomicLong(0);

        new Thread(() -> {
            try {
                for(int i = 0; i < objs.length; i++)
                    q.put(objs[i]);
                putFinishedMillis.set(System.currentTimeMillis());
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }).start();

        for(int i = 0; i < objs.length; i++) {
            Thread.sleep(100);
            Assert.assertEquals(objs[i], q.take());
        }

        // the put should end just after 1.2sec mark, since 12 elements have to wait
        // the total time to process should be 2 sec
        // so the gap should be just under 0.8 sec

        Assert.assertTrue((System.currentTimeMillis() - putFinishedMillis.get()) > 750);
        Assert.assertTrue((System.currentTimeMillis() - putFinishedMillis.get()) < 850);
        Assert.assertNull(q.peek());
    }
}

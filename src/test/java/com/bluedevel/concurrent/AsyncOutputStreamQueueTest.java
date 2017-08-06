package com.bluedevel.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AsyncOutputStreamQueueTest {

    class DummyOutputStream extends ByteArrayOutputStream {
        public volatile boolean wasClosed = false;
        @Override public void close() throws IOException { super.close(); wasClosed = true; }

        public volatile boolean wasFlushed = false;
        @Override public void flush() throws IOException { super.flush(); wasFlushed = true; }
    }
        

    @Test
    public void testWriteMultipleElements() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.write(new byte[] { 'a' });
        out.write(new byte[] { 'b' });
        out.write(new byte[] { 'c' });
        out.write(new byte[] { 'd' });
        out.write(new byte[] { 'e' });
        out.write(new byte[] { 'f' });
        out.write(new byte[] { 'g' });
        out.close();

        Thread.sleep(10);

        Assert.assertEquals(7, dummy.toByteArray().length);
        Assert.assertEquals('a', dummy.toByteArray()[0]);
        Assert.assertEquals('b', dummy.toByteArray()[1]);
        Assert.assertEquals('c', dummy.toByteArray()[2]);
        Assert.assertEquals('d', dummy.toByteArray()[3]);
        Assert.assertEquals('e', dummy.toByteArray()[4]);
        Assert.assertEquals('f', dummy.toByteArray()[5]);
        Assert.assertEquals('g', dummy.toByteArray()[6]);
        Assert.assertTrue(dummy.wasFlushed);
        Assert.assertTrue(dummy.wasClosed);

    }

    @Test
    public void testWriteOneElement() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.write(new byte[] { 'a' });
        Thread.sleep(10);

        Assert.assertEquals(1, dummy.toByteArray().length);
        Assert.assertEquals('a', dummy.toByteArray()[0]);

        out.close();
    }

    @Test
    public void testWriteZeroElementsThenClose() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.write(new byte[0]);
        out.write(new byte[0], 0, 0);
        Thread.sleep(10);

        Assert.assertFalse(dummy.wasClosed);

        out.close();
    }

    @Test
    public void testWriteZeroLengthDoesNotClose() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.write(new byte[0]);
        out.write(new byte[0], 0, 0);
        Thread.sleep(10);

        Assert.assertFalse(dummy.wasClosed);

        out.close();
    }

    @Test
    public void testCloseTriggersFlush() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.close();
        Thread.sleep(10);

        Assert.assertTrue(dummy.wasFlushed);
        Assert.assertEquals(0, dummy.toByteArray().length);
    }

    @Test
    public void testCloseTriggersClose() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        AsyncOutputStreamQueue out = new AsyncOutputStreamQueue(dummy);

        out.close();
        Thread.sleep(10);

        Assert.assertTrue(dummy.wasClosed);
        Assert.assertEquals(0, dummy.toByteArray().length);
    }
}

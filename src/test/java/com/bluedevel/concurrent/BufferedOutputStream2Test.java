package com.bluedevel.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BufferedOutputStream2Test {

    class DummyOutputStream extends ByteArrayOutputStream {
        public volatile boolean wasClosed = false;
        @Override public void close() throws IOException { super.close(); wasClosed = true; }

        public volatile boolean wasFlushed = false;
        @Override public void flush() throws IOException { super.flush(); wasFlushed = true; }
    }

    @Test
    public void testOneSmallItemDoesNotComeThrough() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        BufferedOutputStream2 out = new BufferedOutputStream2(dummy);

        out.write('a');
        Thread.sleep(10);

        Assert.assertEquals(0, dummy.toByteArray().length);
        Assert.assertFalse(dummy.wasFlushed);
        Assert.assertFalse(dummy.wasClosed);
    }

    @Test
    public void testManySmallItemsComeAsChunk() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        BufferedOutputStream2 out = new BufferedOutputStream2(dummy);

        for(int i = 0; i < 32000; i++)
            out.write('a');
        Thread.sleep(10);

        // should have had one flush exactly, of one less than buffer size
        byte[] flushed = dummy.toByteArray();
        Assert.assertEquals(24576, flushed.length);
        for(int i = 0; i < flushed.length; i++)
            Assert.assertEquals('a', flushed[i]);

        Assert.assertFalse(dummy.wasClosed);
    }

    @Test
    public void testCloseWorks() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        BufferedOutputStream2 out = new BufferedOutputStream2(dummy);

        for(int i = 0; i < 32000; i += 10) {
            out.write(new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' });
        }

        out.close();
        Thread.sleep(10);

        // should have had two flushes, and be at full length
        byte[] flushed = dummy.toByteArray();
        Assert.assertEquals(32000, flushed.length);
        Assert.assertTrue(dummy.wasFlushed);
        for(int i = 0; i < flushed.length; i += 10) {
            Assert.assertEquals('a', flushed[i + 0]);
            Assert.assertEquals('b', flushed[i + 1]);
            Assert.assertEquals('c', flushed[i + 2]);
            Assert.assertEquals('d', flushed[i + 3]);
            Assert.assertEquals('e', flushed[i + 4]);
            Assert.assertEquals('f', flushed[i + 5]);
            Assert.assertEquals('g', flushed[i + 6]);
            Assert.assertEquals('h', flushed[i + 7]);
            Assert.assertEquals('i', flushed[i + 8]);
            Assert.assertEquals('j', flushed[i + 9]);
        }

        Assert.assertTrue(dummy.wasClosed);
    }

    @Test
    public void testFlushWorks() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        BufferedOutputStream2 out = new BufferedOutputStream2(dummy);

        for(int i = 0; i < 32000; i++)
            out.write('a');

        out.flush();
        Thread.sleep(10);

        // should have had two flushes, and be at full length
        byte[] flushed = dummy.toByteArray();
        Assert.assertEquals(32000, flushed.length);
        Assert.assertTrue(dummy.wasFlushed);
        for(int i = 0; i < flushed.length; i++)
            Assert.assertEquals('a', flushed[i]);

        Assert.assertFalse(dummy.wasClosed);
    }
}

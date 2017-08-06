package com.bluedevel.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

public class ParallelGZIPOutputStreamTest {

    ExecutorService threads = Executors.newCachedThreadPool();

    class DummyOutputStream extends ByteArrayOutputStream {
        public volatile boolean wasClosed = false;
        @Override public void close() throws IOException { super.close(); wasClosed = true; }

        public volatile boolean wasFlushed = false;
        @Override public void flush() throws IOException { super.flush(); wasFlushed = true; }
    }

    @Test
    public void testZeroByteStream() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        ParallelGZIPOutputStream out = new ParallelGZIPOutputStream(dummy, threads);

        out.flush();
        out.close();

        Thread.sleep(100);

        GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(dummy.toByteArray()));
        byte[] result = readInputStream(input);

        Assert.assertEquals(0, result.length);
        Assert.assertTrue(dummy.wasClosed);
    }

    @Test
    public void test1ByteStream() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        ParallelGZIPOutputStream out = new ParallelGZIPOutputStream(dummy, threads);

        out.write('a');
        out.flush();
        out.close();

        Thread.sleep(100);

        byte[] result = readInputStream(new GZIPInputStream(new ByteArrayInputStream(dummy.toByteArray())));

        Assert.assertEquals(1, result.length);
        Assert.assertEquals('a', result[0]);
        Assert.assertTrue(dummy.wasClosed);
    }

    @Test
    public void test1000ByteStream() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        ParallelGZIPOutputStream out = new ParallelGZIPOutputStream(dummy, threads);

        Random r = new Random();
        byte[] raw = new byte[1000];
        r.nextBytes(raw);

        out.write(raw);
        out.flush();
        out.close();
        
        Thread.sleep(100);

        byte[] result = readInputStream(new GZIPInputStream(new ByteArrayInputStream(dummy.toByteArray())));

        Assert.assertArrayEquals(raw, result);
    }

    @Test
    public void testMany1000ByteChunks() throws Exception {
        DummyOutputStream dummy = new DummyOutputStream();
        ParallelGZIPOutputStream out = new ParallelGZIPOutputStream(dummy, threads);

        Random r = new Random();
        byte[] raw = new byte[1000*1000];
        r.nextBytes(raw);

        for(int i = 0; i < 1000000; i += 1000) {
            out.write(raw, i, 1000);
        }

        out.flush();
        out.close();

        // let it catch up
        Thread.sleep(1000);

        byte[] result = readInputStream(new GZIPInputStream(new ByteArrayInputStream(dummy.toByteArray())));

        Assert.assertArrayEquals(raw, result);
    }

    private byte[] readInputStream(InputStream input) throws IOException {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int len;
        while ((len = input.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        out.close();
        return out.toByteArray();
    }

}

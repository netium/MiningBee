package org.ctp.server.storageengine.lsm.utils;

import org.ctp.server.storageengine.lsm.sstable.SSTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SSTableDescriptorTest {

    @Test
    public void testWriteSSTableDescriptor() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        SSTableDescriptor.writeSSTableDescriptor(byteArrayOutputStream);

        byte[] buf = byteArrayOutputStream.toByteArray();

        Assert.assertEquals(buf.length, 11 + 1 + 1);
        Assert.assertEquals(buf[buf.length - 2], 1);
        Assert.assertEquals(buf[buf.length - 1], 0);
    }

    @Test
    public void testIsValidSSTableDescriptorValidCase() throws IOException {
        byte[] buf = new byte[] {'Z', 'E', 'U', 'S', 'S', 'S', 'T', 'A', 'B', 'L', 'E', 1, 0};
        boolean ret = SSTableDescriptor.isValidDescriptor(buf, 0);
        Assert.assertTrue(ret);
    }

    @Test
    public void testIsValidSSTableDescriptorInvalidVersion() throws IOException {
        byte[] buf = new byte[] {'Z', 'E', 'U', 'S', 'S', 'S', 'T', 'A', 'B', 'L', 'E', 2, 0};
        boolean ret = SSTableDescriptor.isValidDescriptor(buf, 0);
        Assert.assertFalse(ret);
    }

    @Test
    public void testIsValidSSTableDescriptorInvalidMagic() throws IOException {
        byte[] buf = new byte[] {'Z', 'E', 'U', 'S', 'S', 'X', 'T', 'A', 'B', 'L', 'E', 1, 0};
        boolean ret = SSTableDescriptor.isValidDescriptor(buf, 0);
        Assert.assertFalse(ret);
    }

    @Test
    public void testWriteAndThenValid() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        SSTableDescriptor.writeSSTableDescriptor(byteArrayOutputStream);
        byte[] buf = byteArrayOutputStream.toByteArray();
        boolean ret = SSTableDescriptor.isValidDescriptor(buf, 0);
        Assert.assertTrue(ret);
    }
}

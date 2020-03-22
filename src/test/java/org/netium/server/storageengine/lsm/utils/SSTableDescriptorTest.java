/*
 * Copyright (c) 2018.
 *
 * Author: Netium (Bo Zhou)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.netium.server.storageengine.lsm.utils;

import org.netium.server.storageengine.lsm.sstable.SSTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SSTableDescriptorTest {

    @Test
    public void testWriteSSTableDescriptor() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        SSTableDescriptor.writeSSTableDescriptor(dataOutputStream);

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
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        SSTableDescriptor.writeSSTableDescriptor(dataOutputStream);
        byte[] buf = byteArrayOutputStream.toByteArray();
        boolean ret = SSTableDescriptor.isValidDescriptor(buf, 0);
        Assert.assertTrue(ret);
    }
}

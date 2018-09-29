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

package org.ctp.server.storageengine.lsm.utils;

import org.ctp.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KeyValuePairCoderTest {
    @Test
    public void testPackToByteArray() throws IOException {
        final String key = "a";
        final String value = "aa";
        Pair<String, String> keyValuePair = new Pair<>(key, value);
        byte[] buf = KeyValuePairCoder.pack(keyValuePair);

        Assert.assertEquals(buf.length, 2 + key.length() + 4 + value.length());
        Assert.assertTrue(buf[0] == 0);
        Assert.assertTrue(buf[1] == 1);
        Assert.assertTrue(buf[2] == 'a');
        Assert.assertTrue(buf[3] == 0);
        Assert.assertTrue(buf[4] == 0);
        Assert.assertTrue(buf[5] == 0);
        Assert.assertTrue(buf[6] == 2);
        Assert.assertTrue(buf[7] == 'a');
        Assert.assertTrue(buf[8] == 'a');
    }

    @Test
    public void testPackToByteArrayNullValue() throws IOException {
        final String key = "a";
        final String value = null;

        Pair<String, String> keyValuePair = new Pair<>(key, value);
        byte[] buf = KeyValuePairCoder.pack(keyValuePair);

        Assert.assertEquals(buf.length, 2 + key.length() + 4 + 0);
        Assert.assertTrue(buf[0] == 0);
        Assert.assertTrue(buf[1] == 1);
        Assert.assertTrue(buf[2] == 'a');
        Assert.assertTrue(buf[3] == -1);
        Assert.assertTrue(buf[4] == -1);
        Assert.assertTrue(buf[5] == -1);
        Assert.assertTrue(buf[6] == -1);
    }

    @Test
    public void testPackToByteArrayValueEmptyString() throws IOException {
        final String key = "a";
        final String value = "";

        Pair<String, String> keyValuePair = new Pair<>(key, value);
        byte[] buf = KeyValuePairCoder.pack(keyValuePair);

        Assert.assertEquals(buf.length, 2 + key.length() + 4 + 0);
        Assert.assertTrue(buf[0] == 0);
        Assert.assertTrue(buf[1] == 1);
        Assert.assertTrue(buf[2] == 'a');
        Assert.assertTrue(buf[3] == 0);
        Assert.assertTrue(buf[4] == 0);
        Assert.assertTrue(buf[5] == 0);
        Assert.assertTrue(buf[6] == 0);
    }

    @Test
    public void testUnpackByteArrayToPairNormal() throws IOException {
        final byte[] buf = new byte[] {0, 1, 'a', 0, 0, 0, 2, 'a', 'b'};
        Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(buf, 0, buf.length);
        Assert.assertEquals(keyValuePair.getKey(), "a");
        Assert.assertEquals(keyValuePair.getValue(), "ab");
    }

    @Test
    public void testUnpackByteArrayToPairValueEmptyString() throws IOException {
        final byte[] buf = new byte[] {0, 1, 'a', 0, 0, 0, 0};
        Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(buf, 0, buf.length);
        Assert.assertEquals(keyValuePair.getKey(), "a");
        Assert.assertEquals(keyValuePair.getValue(), "");
    }

    @Test
    public void testUnpackByteArrayToPairValueNull() throws IOException {
        final byte[] buf = new byte[] {0, 1, 'a', -1, -1, -1, -1};
        Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(buf, 0, buf.length);
        Assert.assertEquals(keyValuePair.getKey(), "a");
        Assert.assertEquals(keyValuePair.getValue(), null);
    }
}

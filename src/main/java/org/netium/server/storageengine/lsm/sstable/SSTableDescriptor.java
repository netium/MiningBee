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

package org.netium.server.storageengine.lsm.sstable;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class SSTableDescriptor {
    private final static byte[] MAGIC = "ZEUSSSTABLE".getBytes(StandardCharsets.US_ASCII);

    private final static byte MAJOR_VER = 1;
    private final static byte MINOR_VER = 0;

    private final static int DESCRIPTOR_SIZE = MAGIC.length + Byte.BYTES + Byte.BYTES; // Magic + majorVer + minorVer

    public static boolean isValidDescriptor(DataInput dataInput) throws IOException {
        if (dataInput == null)
            throw new IllegalArgumentException("The dataInput is null");

        for (int i = 0; i < MAGIC.length; i++) {
            if (dataInput.readByte() != MAGIC[i])
                return false;
        }

        if (dataInput.readByte() != MAJOR_VER) return false;
        if (dataInput.readByte() != MINOR_VER) return false;

        return true;
    }

    public static boolean isValidDescriptor(byte[] buf, int start) {
        if (buf == null)
            throw new IllegalArgumentException("The buf is null");

        if (start + DESCRIPTOR_SIZE > buf.length)
            throw new IllegalArgumentException("The buf does not have enough data");

        for (int i = 0; i < MAGIC.length; i++) {
            if (buf[start++] != MAGIC[i])
                return false;
        }

        if (buf[start++] != MAJOR_VER) return false;
        if (buf[start++] != MINOR_VER) return false;

        return true;
    }

    public static int writeSSTableDescriptor(DataOutput dataOutput) throws IOException {
        if (dataOutput == null)
            throw new IllegalArgumentException("The outputStream is null");

        dataOutput.write(MAGIC);
        dataOutput.writeByte(MAJOR_VER);
        dataOutput.writeByte(MINOR_VER);

        return DESCRIPTOR_SIZE;
    }

}

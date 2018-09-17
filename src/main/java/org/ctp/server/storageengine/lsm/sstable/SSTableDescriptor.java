package org.ctp.server.storageengine.lsm.sstable;

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

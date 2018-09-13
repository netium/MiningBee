package org.ctp.server.storageengine.lsm.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class SSTableDescriptor {
    private final static byte[] MAGIC = "ZEUSSSTABLE".getBytes(StandardCharsets.US_ASCII);

    private final static byte MAJOR_VER = 1;
    private final static byte MINOR_VER = 0;

    private final static int DESCRIPTOR_SIZE = MAGIC.length + Byte.BYTES + Byte.BYTES; // Magic + majorVer + minorVer

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

    public static boolean isValidDescriptor(InputStream inputStream) throws IOException {
        if (inputStream == null)
            throw new IllegalArgumentException("The inputStream is null");

        for (int i = 0; i < MAGIC.length; i++) {
            if (inputStream.read() != MAGIC[i])
                return false;
        }

        if (inputStream.read() != MAJOR_VER) return false;
        if (inputStream.read() != MINOR_VER) return false;

        return true;
    }

    public static int writeSSTableDescriptor(OutputStream outputStream) throws IOException {
        if (outputStream == null)
            throw new IllegalArgumentException("The outputStream is null");

        outputStream.write(MAGIC);
        outputStream.write(MAJOR_VER);
        outputStream.write(MINOR_VER);

        return DESCRIPTOR_SIZE;
    }

}

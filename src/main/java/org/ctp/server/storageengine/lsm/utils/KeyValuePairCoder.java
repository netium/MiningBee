package org.ctp.server.storageengine.lsm.utils;

import org.ctp.util.Pair;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class KeyValuePairCoder {
    public final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private KeyValuePairCoder() {}

    public static byte[] pack(Pair<String, String> keyValuePair) throws IOException {
        if (keyValuePair == null)
            throw new IllegalArgumentException("keyValuePair is null");

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        pack(keyValuePair, byteArrayOutputStream);

        return byteArrayOutputStream.toByteArray();
    }

    public static Pair<String, String> unpack(byte[] buffer, int start, int length) throws IOException {
        if (buffer == null)
            throw new IllegalArgumentException("buffer is null");

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer, start, length);

        return unpack(byteArrayInputStream);
    }

    public static int pack(Pair<String, String> keyValuePair, OutputStream outputStream) throws IOException {
        int nbytes = 0;

        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        byte[] keyBuf = encodeString(keyValuePair.getKey());
        if (keyBuf.length > Short.MAX_VALUE)
            throw new IllegalArgumentException("The encoded key length exceeds " + Short.MAX_VALUE + " bytes");

        dataOutputStream.writeShort(keyBuf.length);
        dataOutputStream.write(keyBuf);
        nbytes += (Short.BYTES + keyBuf.length);

        byte[] valueBuf = encodeString(keyValuePair.getValue());
        if (valueBuf == null) {
            dataOutputStream.writeInt(-1);
            nbytes += Integer.BYTES;
        }
        else {
            dataOutputStream.writeInt(valueBuf.length);
            dataOutputStream.write(valueBuf);
            nbytes += (Integer.BYTES + valueBuf.length);
        }

        return nbytes;
    }

    public static Pair<String, String> unpack(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        short keyLength = dataInputStream.readShort();
        if (keyLength <= 0)
            throw new NumberFormatException("The key length is <= 0");

        byte[] keyBuf = new byte[keyLength];
        dataInputStream.readFully(keyBuf);

        String key = decodeString(keyBuf);

        int valueLength = dataInputStream.readInt();
        byte[] valueBuf = null;
        if (valueLength >= 0)
            valueBuf = new byte[valueLength];

        if (valueBuf != null)
            dataInputStream.readFully(valueBuf);

        String value = valueBuf == null ? null : decodeString(valueBuf);

        return new Pair<>(key, value);
    }

    public static byte[] encodeString(String str) {
        if (str == null)
            return null;

        return str.getBytes(DEFAULT_CHARSET);
    }

    public static String decodeString(byte[] buffer) {
        if (buffer == null)
            return null;

        return new String(buffer, DEFAULT_CHARSET);
    }

    public static String decodeString(byte[] buffer, int start, int length) {
        if (buffer == null)
            return null;

        return new String(buffer, start, length, DEFAULT_CHARSET);
    }
}

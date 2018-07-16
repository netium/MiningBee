package org.ctp.core.storageengine.lsm;

import javafx.util.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;

public class SSTable {
    private final byte[] HEADER = {'C', 'T', 'P', 'D', 'K', 'V'};

    private final String filename;


    public SSTable(String sstableFilename) {
        filename = sstableFilename;
    }

    public static SSTable flush(Memtable memtable, String sstableFilename) throws IOException {
        SSTable newSSTable = new SSTable(sstableFilename);
        newSSTable.flush(memtable);
        return newSSTable;
    }

    public InMemIndex buildIndex() throws IOException {
        try (DataInputStream inputStream = new DataInputStream(new FileInputStream(filename))) {
            verifyIntegrity(inputStream);
        }

        throw new NotImplementedException();
    }

    private void flush(Memtable memtable) throws IOException {
        File file = new File(filename);
        if (file.exists() && file.isFile()) {
            throw new FileAlreadyExistsException("The file " + filename + " was already existed");
        }
        try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(filename))) {
            for (Pair<String, String> entry : memtable) {
                appendItem(outputStream, entry.getKey(), entry.getValue());
            }
        }
    }

    private void appendItem(DataOutputStream outputStream, String key, String value) throws IOException {
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("The key is null or empty");
        }
        if (value != null && value.length() == 0) {
            throw new IllegalArgumentException("The value is empty");
        }

        byte[] keyBytes = key.getBytes(Charset.defaultCharset());
        if (keyBytes.length > 127) {
            throw new IllegalArgumentException("The key exceed the length");
        }

        byte[] valueBytes = value.getBytes(Charset.defaultCharset());
        if (valueBytes.length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("The value exceed the length");
        }

        outputStream.writeByte(keyBytes.length);
        outputStream.write(keyBytes);
        outputStream.write(valueBytes.length);
        if (valueBytes.length > 0) {
            outputStream.write(valueBytes);
        }
    }

    private Pair<String, String> readItem(DataInputStream inputStream) throws IOException {
        int keyLength = inputStream.readByte();
        if (keyLength < 0) {
            throw new IllegalArgumentException();
        }
        byte[] keyBuf = new byte[keyLength];
        inputStream.read(keyBuf);
        String key = new String(keyBuf);

        int valueLength = inputStream.readInt();
        if (valueLength < 0) {
            throw new IllegalArgumentException();
        }
        String value = null;
        if (valueLength > 0) {
            byte[] valueBuf = new byte[valueLength];
            inputStream.read(valueBuf);
            value = new String(valueBuf);
        }

        return new Pair<>(key, value);
    }

    private void verifyIntegrity(DataInputStream inputStream) throws IOException {
        checkHeader(inputStream);
    }

    private boolean checkHeader(DataInputStream inputStream) throws IOException {
        byte[] header = new byte[HEADER.length];
        inputStream.read(header);

        for (int i = 0; i < header.length; i++) {
            if (header[i] != HEADER[i]) {
                return false;
            }
        }

        return true;
    }

    private void setHeader(OutputStream outputStream) throws IOException {
        outputStream.write(HEADER);
    }

    private int getHeaderSize() {
        return HEADER.length;
    }

}

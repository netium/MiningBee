package org.ctp.core.storageengine.lsm;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;

public class SSTable implements Iterable<Pair<String, Long>> {
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

    public Pair<String, String> readItem(long offset) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            file.seek(offset);
            return readItem(file);
        }
    }

    private void flush(Memtable memtable) throws IOException {
        File file = new File(filename);
        if (file.exists() && file.isFile()) {
            throw new FileAlreadyExistsException("The file " + filename + " was already existed");
        }
        try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(file))) {
            setHeader(outputStream);
            for (Pair<String, String> entry : memtable) {
                appendItem(outputStream, entry.getKey(), entry.getValue());
            }
        }

        try (RandomAccessFile dataOutput = new RandomAccessFile(file, "rw")) {
            setHeader(dataOutput);
            for (Pair<String, String> entry : memtable) {
                appendItem(dataOutput, entry.getKey(), entry.getValue());
            }
        }
    }

    private void appendItem(DataOutput outputStream, String key, String value) throws IOException {
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

    private Pair<String, String> readItem(DataInput dataInput) throws IOException {
        int keyLength = dataInput.readByte();
        if (keyLength < 0) {
            throw new IllegalArgumentException();
        }
        byte[] keyBuf = new byte[keyLength];
        dataInput.readFully(keyBuf);
        String key = new String(keyBuf);

        int valueLength = dataInput.readInt();
        if (valueLength < 0) {
            throw new IllegalArgumentException();
        }
        String value = null;
        if (valueLength > 0) {
            byte[] valueBuf = new byte[valueLength];
            dataInput.readFully(valueBuf);
            value = new String(valueBuf);
        }

        return new Pair<>(key, value);
    }

    private void verifyIntegrity(DataInputStream inputStream) throws IOException {
        if (!checkHeader(inputStream)) {
            throw new BadSSTableException();
        }
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

    private void setHeader(DataOutput dataOutput) throws IOException {
        dataOutput.write(HEADER);
    }

    private int getHeaderSize() {
        return HEADER.length;
    }

    @Override
    public Iterator<Pair<String, Long>> iterator() {
        try {
            return new SSTableIterator();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private class SSTableIterator implements Iterator<Pair<String, Long>> {
        private DataInputStream inputStream;
        private long currentOffset;

        public SSTableIterator() throws IOException {
            inputStream = new DataInputStream(new FileInputStream(filename));
            verifyIntegrity(inputStream);
            currentOffset = 0;
        }

        @Override
        public boolean hasNext() {
            try {
                if (inputStream.available() <= 0) {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
            currentOffset += getHeaderSize();
            return true;
        }

        @Override
        public Pair<String, Long> next() {
            try {
                return readItemOffset();
            } catch (IOException e) {
                return new Pair<>(null, null);
            }
        }

        private Pair<String, Long> readItemOffset() throws IOException {
            final long offset = currentOffset;
            int keyLength = inputStream.readByte();
            if (keyLength < 0) {
                throw new IllegalArgumentException();
            }
            currentOffset += Byte.BYTES;

            byte[] keyBuf = new byte[keyLength];
            inputStream.read(keyBuf);
            String key = new String(keyBuf);
            currentOffset += keyBuf.length;

            int valueLength = inputStream.readInt();
            if (valueLength < 0) {
                throw new IllegalArgumentException();
            }
            currentOffset += Integer.BYTES;

            String value = null;
            if (valueLength > 0) {
                byte[] valueBuf = new byte[valueLength];
                inputStream.read(valueBuf);
                value = new String(valueBuf);
                currentOffset += valueBuf.length;
            }

            return new Pair<>(key, offset);
        }
    }
}

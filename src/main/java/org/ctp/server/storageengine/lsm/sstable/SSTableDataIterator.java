package org.ctp.server.storageengine.lsm.sstable;

import org.ctp.server.storageengine.lsm.utils.KeyValuePairCoder;
import org.ctp.util.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class SSTableDataIterator implements Iterator<Pair<String, String>>, AutoCloseable {
    private final File file;
    private final RandomAccessFile sstable;

    private long dataStartOffset;
    private long dataEndOffset;

    public SSTableDataIterator(File file) throws IOException {
        this.file = file;
        sstable = new RandomAccessFile(file, "r");

        if (!SSTableDescriptor.isValidDescriptor(sstable)) {
            throw new BadSSTableException("The file: " + file.getAbsolutePath() + " is not a valid SSTable file");
        }

        moveToDataRegion(sstable);
    }

    private void moveToDataRegion(RandomAccessFile sstable) throws IOException {
        while (sstable.getFilePointer() < sstable.length()) {
            SSTableSectionHeader sectionHeader = SSTableSectionHeader.loadSectionHeader(sstable);
            if (SSTableConstants.DATA_SECTION_NAME_STRING.equals(sectionHeader.getNameString())) {
                dataStartOffset = sstable.getFilePointer();
                dataEndOffset = dataStartOffset + sectionHeader.getSize();
                return;
            }
            else {
                sstable.seek(sstable.getFilePointer() + sectionHeader.getSize());
            }
        }
        throw new BadSSTableException("Cannot find data section");
    }


    @Override
    public boolean hasNext() {
        try {
            return sstable.getFilePointer() < dataEndOffset;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Pair<String, String> next() {
        try {
            Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(sstable);
            return keyValuePair;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        sstable.close();
    }
}

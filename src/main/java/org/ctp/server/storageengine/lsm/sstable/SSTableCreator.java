package org.ctp.server.storageengine.lsm.sstable;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.ctp.server.storageengine.lsm.utils.KeyValuePairCoder;
import org.ctp.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public final class SSTableCreator {
    private final Logger logger = LoggerFactory.getLogger(SSTableCreator.class);

    private final BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 500, 0.1);
    private final File file;
    private RandomAccessFile sstable;
    private final SSTableSectionHeader dataSectionHeader;

    private String previousKey = "";

    public SSTableCreator(File targetFile) throws IOException {
        this.file = targetFile;
        sstable = new RandomAccessFile(file, "w");
        SSTableDescriptor.writeSSTableDescriptor(sstable);
        dataSectionHeader = new SSTableSectionHeader(new byte[] {'D', 'A', 'T'}, sstable.getFilePointer(), 0);
        dataSectionHeader.write(sstable);
    }

    public void write(Pair<String, String> keyValuePair) throws IOException {
        if (keyValuePair == null)
            throw new IllegalArgumentException("The keyValuePair is null");

        final String key = keyValuePair.getKey();
        if (key == null)
            throw new IllegalArgumentException("The key is null");
        if (key.compareTo(previousKey) <= 0)
            throw new IllegalArgumentException("The ascending key order is not hold");
        previousKey = key;

        int nbytes = KeyValuePairCoder.pack(keyValuePair, Channels.newOutputStream(sstable.getChannel()));
        dataSectionHeader.setSize(dataSectionHeader.getSize() + nbytes);

        filter.put(key);
    }

    public SSTable flush() {
        return null;
    }
}

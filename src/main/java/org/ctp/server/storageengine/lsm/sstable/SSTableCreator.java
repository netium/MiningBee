package org.ctp.server.storageengine.lsm.sstable;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private SSTableCreatorState state;

    private final BloomFilter<String> filter;
    private final File file;
    private RandomAccessFile sstable;
    private final SSTableSectionHeader dataSectionHeader;
    private int nDataItems = 0;

    private String previousKey = "";

    public SSTableCreator(File targetFile, int approxItems) throws IOException {
        this.file = targetFile;
        filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), approxItems, 0.1);
        sstable = new RandomAccessFile(file, "rw");
        SSTableDescriptor.writeSSTableDescriptor(sstable);
        dataSectionHeader = new SSTableSectionHeader(SSTableConstants.DATA_SECTION_NAME_BYTES, sstable.getFilePointer(), 0);
        dataSectionHeader.write(sstable);
        state = SSTableCreatorState.DATA_WRITING;
    }

    public void write(Pair<String, String> keyValuePair) throws IOException {
        if (state != SSTableCreatorState.DATA_WRITING)
            throw new BadSSTableException("The sstable is not in data writing status");

        if (keyValuePair == null)
            throw new IllegalArgumentException("The keyValuePair is null");

        final String key = keyValuePair.getKey();
        if (key == null)
            throw new IllegalArgumentException("The key is null");
        if (key.compareTo(previousKey) <= 0)
            throw new IllegalArgumentException("The ascending key order is not hold");
        previousKey = key;

        int nbytes = KeyValuePairCoder.pack(keyValuePair, sstable);
        dataSectionHeader.setSize(dataSectionHeader.getSize() + nbytes);
        nDataItems++;

        filter.put(key);
    }

    public NewSSTable flush() throws IOException {
        dataSectionHeader.writeByOffset(sstable);

        flushBloomFilter();
        flushSummary();

        sstable.close();
        state = SSTableCreatorState.CLOSED;

        return new NewSSTable(file);
    }

    private void flushBloomFilter() throws IOException {
        SSTableSectionHeader bloomFilterSectionHeader = new SSTableSectionHeader(
                SSTableConstants.BLOOMFILTER_SECTION_NAME_BYTES,
                sstable.getFilePointer(),
                0
        );

        bloomFilterSectionHeader.write(sstable);
        long bloomFilterDataOffset = sstable.getFilePointer();

        filter.writeTo(Channels.newOutputStream(sstable.getChannel()));
        long bloomFilterDataEndOffset = sstable.getFilePointer();

        long bloomFilterDataSize = bloomFilterDataEndOffset - bloomFilterDataOffset;

        bloomFilterSectionHeader.setSize(bloomFilterDataSize);
        bloomFilterSectionHeader.writeByOffset(sstable);
    }

    private void flushSummary() throws IOException {
        SSTableSectionHeader summarySectionHeader = new SSTableSectionHeader(
                SSTableConstants.SUMMARY_SECTION_NAME_BYTES,
                sstable.getFilePointer(),
                0
        );
        summarySectionHeader.write(sstable);

        long summaryContentStartOffset = sstable.getFilePointer();

        SSTableSummaryInfo summary = new SSTableSummaryInfo();
        summary.setDataItems(nDataItems);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(sstable, summary);

        long summaryContentEndOffset = sstable.getFilePointer();

        summarySectionHeader.setSize(summaryContentEndOffset - summaryContentStartOffset);

        summarySectionHeader.writeByOffset(sstable);
    }
}

enum SSTableCreatorState {
    CLOSED,
    DATA_WRITING,
}

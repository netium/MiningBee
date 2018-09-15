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
import java.util.*;

public class NewSSTable implements AutoCloseable, Iterable<Pair<String, String>> {
    private final Logger LOGGER = LoggerFactory.getLogger(NewSSTable.class);

    private final File file;
    private final RandomAccessFile sstableAccessFile;

    private BloomFilter<String> filter;

    private SortedMap<String, Long> index = new TreeMap<String, Long>();

    private HashMap<String, SSTableSectionHeader> sections = new HashMap<>();

    private long nItems = -1;

    public long getNumOfItems() {
        return nItems;
    }

    public NewSSTable(File file) throws IOException {
        if (file == null)
            throw new IllegalArgumentException("The file is null");

        LOGGER.info("Loading NewSSTable: {}", file.getAbsolutePath());

        this.file = file;

        sstableAccessFile = new RandomAccessFile(this.file, "r");

        if (!SSTableDescriptor.isValidDescriptor(sstableAccessFile))
            throw new BadSSTableException("The file " + file.getCanonicalPath() + " is not a valid NewSSTable");

        while(sstableAccessFile.getFilePointer() < sstableAccessFile.length()) {
            SSTableSectionHeader sectionHeader = SSTableSectionHeader.loadSectionHeader(sstableAccessFile);
            if (sections.containsKey(sectionHeader.getNameString()))
                throw new BadSSTableException("The section " + sectionHeader.getNameString() + " is duplicated");

            sections.put(sectionHeader.getNameString(), sectionHeader);

            loadContent(sstableAccessFile, sectionHeader);
        }

    }

    public Pair<String, String> get(String key) throws IOException {
        if (key == null)
            throw new IllegalArgumentException("The key is null");

        if (filter != null && !filter.mightContain(key))
            return null;

        if (!index.containsKey(key))
            return null;

        try (RandomAccessFile readFile = new RandomAccessFile(this.file, "r")) {
            long offset = index.get(key);
            readFile.seek(offset);
            Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(Channels.newInputStream(readFile.getChannel()));
            return keyValuePair;
        }
    }

    private void loadContent(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        switch (sectionHeader.getNameString()) {
            case SSTableConstants.DATA_SECTION_NAME_STRING:
                loadData(sstableAccessFile, sectionHeader);
                break;
            case SSTableConstants.INDEX_SECTION_NAME_STRING:
                loadIndex(sstableAccessFile, sectionHeader);
                break;
            case SSTableConstants.BLOOMFILTER_SECTION_NAME_STRING:
                loadBloomFilter(sstableAccessFile, sectionHeader);
                break;
            case SSTableConstants.SUMMARY_SECTION_NAME_STRING:
                loadSummary(sstableAccessFile, sectionHeader);
                break;
            default:
                LOGGER.warn("Unknown section: {}, bypass it", sectionHeader.getNameString());
                bypassSection(sstableAccessFile, sectionHeader);
                break;
        }
    }

    private void bypassSection(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        sstableAccessFile.seek(sectionHeader.getContentOffset() + sectionHeader.getSize());
    }

    private void loadBloomFilter(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        InputStream inputStream = Channels.newInputStream(sstableAccessFile.getChannel());
        filter = BloomFilter.readFrom(inputStream, Funnels.stringFunnel(StandardCharsets.UTF_8));
    }

    private void loadIndex(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        if (sectionHeader.getSize() != 0)
            throw new BadSSTableException("The index table is not supported yet");
    }

    private void loadSummary(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        SSTableSummaryInfo summary = (SSTableSummaryInfo)objectMapper.readValue(sstableAccessFile, SSTableSummaryInfo.class);
        nItems = summary.getDataItems();
    }

    private void loadData(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        long sectionEndPos = sstableAccessFile.getFilePointer() + sectionHeader.getSize();

        InputStream inputStream = Channels.newInputStream(sstableAccessFile.getChannel());

        while (sstableAccessFile.getFilePointer() < sectionEndPos) {
            long currentPos = sstableAccessFile.getFilePointer();
            Pair<String, String> keyValuePair = KeyValuePairCoder.unpack(inputStream);
            index.put(keyValuePair.getKey(), currentPos);
        }

        assert sstableAccessFile.getFilePointer() == sectionEndPos;
        if (sstableAccessFile.getFilePointer() != sectionEndPos)
            throw new BadSSTableException("The position is misaligned after reading the sstable data");
    }

    @Override
    public void close() throws Exception {
        sstableAccessFile.close();
    }

    @Override
    public Iterator<Pair<String, String>> iterator() {
        try {
            return new SSTableDataIterator(file);
        } catch (IOException e) {
            return null;
        }
    }
}

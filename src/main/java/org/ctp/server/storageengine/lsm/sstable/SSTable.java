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
import java.util.HashMap;
import java.util.SortedMap;

public class SSTable {
    private final Logger LOGGER = LoggerFactory.getLogger(SSTable.class);

    private final File file;
    private final RandomAccessFile sstableAccessFile;

    private BloomFilter<String> filter;

    private SortedMap<String, Long> index;

    private HashMap<String, SSTableSectionHeader> sections = new HashMap<>();

    public SSTable(File file) throws IOException {
        if (file == null)
            throw new IllegalArgumentException("The file is null");

        LOGGER.info("Loading SSTable: {}", file.getAbsolutePath());

        this.file = file;

        sstableAccessFile = new RandomAccessFile(this.file, "r");

        if (!SSTableDescriptor.isValidDescriptor(sstableAccessFile))
            throw new BadSSTableException("The file " + file.getCanonicalPath() + " is not a valid SSTable");

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
            case "DAT":
                loadData(sstableAccessFile, sectionHeader);
                break;
            case "IDX":
                loadIndex(sstableAccessFile, sectionHeader);
                break;
            case "BMF":
                loadBloomFilter(sstableAccessFile, sectionHeader);
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
        DataOutput dataOutput;
    }

    private void loadIndex(RandomAccessFile sstableAccessFile, SSTableSectionHeader sectionHeader) throws IOException {
        if (sectionHeader.getSize() != 0)
            throw new BadSSTableException("The index table is not supported yet");
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
}

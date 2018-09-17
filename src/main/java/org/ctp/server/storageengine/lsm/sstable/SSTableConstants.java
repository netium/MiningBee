package org.ctp.server.storageengine.lsm.sstable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class SSTableConstants {
    public static final Charset SECTION_HEADER_CHARSET = StandardCharsets.US_ASCII;

    public static final String DATA_SECTION_NAME_STRING = "DAT";
    public static final byte[] DATA_SECTION_NAME_BYTES = DATA_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String BLOOMFILTER_SECTION_NAME_STRING = "BLM";
    public static final byte[] BLOOMFILTER_SECTION_NAME_BYTES = BLOOMFILTER_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String INDEX_SECTION_NAME_STRING = "IDX";
    public static final byte[] INDEX_SECTION_NAME_BYTES = INDEX_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    public static final String SUMMARY_SECTION_NAME_STRING = "SUM";
    public static final byte[] SUMMARY_SECTION_NAME_BYTES = SUMMARY_SECTION_NAME_STRING.getBytes(SECTION_HEADER_CHARSET);

    private SSTableConstants() {}
}

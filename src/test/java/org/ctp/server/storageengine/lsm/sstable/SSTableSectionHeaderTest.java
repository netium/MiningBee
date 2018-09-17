package org.ctp.server.storageengine.lsm.sstable;

import org.junit.Test;
import org.testng.Assert;

public class SSTableSectionHeaderTest {

    @Test
    public void testSectionHeaderData() {
        final long offset = 10;
        final long size = 15;

        SSTableSectionHeader sectionHeader = new SSTableSectionHeader(SSTableConstants.DATA_SECTION_NAME_BYTES, offset, size);

        Assert.assertEquals(sectionHeader.getNameString(), SSTableConstants.DATA_SECTION_NAME_STRING);
        Assert.assertEquals(sectionHeader.getOffset(), offset);
        Assert.assertEquals(sectionHeader.getSize(), size);
        Assert.assertEquals(sectionHeader.getContentOffset(), offset + SSTableConstants.DATA_SECTION_NAME_BYTES.length + Long.BYTES);
    }
}

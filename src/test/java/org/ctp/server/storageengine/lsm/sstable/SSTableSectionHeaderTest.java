/*
 * Copyright (c) 2018.
 *
 * Author: Netium (Bo Zhou)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

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

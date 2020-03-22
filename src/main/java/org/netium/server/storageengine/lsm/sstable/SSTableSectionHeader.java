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

package org.netium.server.storageengine.lsm.sstable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class SSTableSectionHeader {
    private static final Charset CHARSET = StandardCharsets.US_ASCII;

    private static final int SECTION_NAME_SIZE = 3;
    private final byte[] sectionName;
    private final long offset;
    private long size;

    public SSTableSectionHeader(byte[] sectionName, long sectionOffset, long sectionSize) {
        if (sectionName == null)
            throw new IllegalArgumentException("The sectionName is null");
        if (sectionName.length != SECTION_NAME_SIZE)
            throw new IllegalArgumentException("The sectionName length is wrong");
        if (sectionSize < 0)
            throw new IllegalArgumentException("The sectionSize is < 0");

        this.sectionName = sectionName;
        this.offset = sectionOffset;
        this.size = sectionSize;
    }

    public byte[] getSectionName() {
        return sectionName;
    }

    public String getNameString() {
        return new String(sectionName, CHARSET);
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getContentOffset() {
        return offset + sectionName.length + Long.BYTES;
    }

    public static SSTableSectionHeader loadSectionHeader(RandomAccessFile dataInput) throws IOException {
        long sectionOffset = dataInput.getFilePointer();

        byte[] header = new byte[SECTION_NAME_SIZE];
        dataInput.readFully(header);

        long sectionSize = dataInput.readLong();
        if (sectionSize < 0)
            throw new BadSSTableException("The section size is < 0");

        if (sectionOffset + sectionSize > dataInput.length())
            throw new BadSSTableException("The section size is invalid");

        return new SSTableSectionHeader(header, sectionOffset, sectionSize);
    }

    public int write(DataOutput dataOutput) throws IOException {
        dataOutput.write(sectionName);
        dataOutput.writeLong(size);

        return SECTION_NAME_SIZE + Long.BYTES;
    }

    public int writeByOffset(RandomAccessFile randomAccessFile) throws IOException {
        long bookmark = randomAccessFile.getFilePointer();
        randomAccessFile.seek(offset);
        int nbytes = write(randomAccessFile);
        randomAccessFile.seek(bookmark);
        return nbytes;
    }
}

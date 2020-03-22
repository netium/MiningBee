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

import org.netium.server.storageengine.lsm.utils.KeyValuePairCoder;
import org.netium.util.Pair;

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

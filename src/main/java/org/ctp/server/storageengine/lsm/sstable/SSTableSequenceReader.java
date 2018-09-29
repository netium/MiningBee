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

import org.ctp.util.Pair;

import java.io.EOFException;
import java.util.Iterator;

public class SSTableSequenceReader {
    private final Iterator<Pair<String, String>> iterator;

    private Pair<String, String> cacheItem;

    public SSTableSequenceReader(Iterator<Pair<String, String>> sstableIterator) {
        iterator = sstableIterator;
    }

    public boolean isEof() {
        return cacheItem == null && !iterator.hasNext();
    }

    public String peekNextKey() throws EOFException {
        chargeCacheItemIfNeeded();

        return cacheItem.getKey();
    }

    public Pair<String, String> read() throws EOFException {
        chargeCacheItemIfNeeded();
        assert cacheItem != null;
        Pair<String, String> item = cacheItem;
        cacheItem = null;
        return item;
    }

    private void chargeCacheItemIfNeeded() throws EOFException {
        if (cacheItem != null)
            return;

        if (iterator.hasNext()) {
            cacheItem = iterator.next();
        } else {
            throw new EOFException();
        }
    }
}

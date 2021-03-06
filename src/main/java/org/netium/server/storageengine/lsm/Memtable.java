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

package org.netium.server.storageengine.lsm;

import org.netium.util.Pair;

import java.util.*;

public class Memtable implements Iterable<Pair<String, String>>  {
    private int version = 0;
    private int rawSize = 0;
    private SortedMap<String, String> map = new TreeMap<String, String>();

    public Memtable() {

    }

    public void put(String key, String value) {
        if (key == null || key.length() == 0)
            throw new IllegalArgumentException();
        if (value == null)
            throw new IllegalArgumentException("The value is null");

        map.put(key, value);
        rawSize += (key.getBytes().length + value.getBytes().length);
        version++;
    }

    public String get(String key) {
        return map.get(key);
    }

    public void delete(String key) {
        if (key == null || key.length() == 0)
            throw new IllegalArgumentException();

        map.put(key, null);
        rawSize += (key.getBytes().length);
        version++;
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public String read(String key) {
        return map.get(key);
    }

    public int getRawDataSize() {
        return rawSize;
    }

    public int size() {
        return map.size();
    }

    @Override
    public Iterator<Pair<String, String>> iterator() {
        return new MemtableIterator(version);
    }

    private class MemtableIterator implements Iterator<Pair<String, String>> {
        private final int snapshotVersion;
        private final Iterator<String> iterator;

        public MemtableIterator(int version) {
            this.snapshotVersion = version;
            iterator = map.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (snapshotVersion != version) {
                throw new IllegalStateException();
            }
            return iterator.hasNext();

        }

        @Override
        public Pair<String, String> next() {
            if (snapshotVersion != version) {
                throw new IllegalStateException();
            }
            String key = iterator.next();
            return new Pair<>(key, map.get(key));
        }
    }
}

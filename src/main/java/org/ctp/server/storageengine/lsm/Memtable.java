package org.ctp.server.storageengine.lsm;

import org.ctp.util.Pair;

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

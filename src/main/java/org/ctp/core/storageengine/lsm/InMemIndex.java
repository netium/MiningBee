package org.ctp.core.storageengine.lsm;

import org.ctp.util.Pair;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class InMemIndex {
    private final SSTable sstable;
    private final SortedMap<String, Long> map = new TreeMap<>();

    public InMemIndex(SSTable sstable) {
        this.sstable = sstable;
        this.build();
    }

    public SSTable getSSTable() {
        return sstable;
    }

    public String get(String key) {
        Long offset = map.get(key);
        if (offset == null)
            return null;
        try {
            return sstable.readItem(offset).getValue();
        } catch (IOException e) {
            throw new InMemIndexReadException(e);
        }
    }

    public boolean contains(String key) {
        return map.containsKey(key);

    }

    private void build() {
        for (Pair<String, Long> entry : sstable) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

}

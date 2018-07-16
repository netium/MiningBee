package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.ArrayList;

public class LsmStorageEngine implements IStorageEngine {
    private final int MEMTABLE_THRESHOLD = 1 * 1024;
    private final ArrayList<InMemIndex> segmentInMemIndexList = new ArrayList();
    private volatile Memtable currentMemtable = new Memtable();
    private volatile Memtable flushingMemtable = null;

    public LsmStorageEngine() {
    }

    @Override
    public void initEngine(String dbFileFolder) {

    }

    @Override
    public boolean put(String key, String value) {
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException();
        }
        if (value == null || value.length() == 0) {
            throw new IllegalArgumentException();
        }

        currentMemtable.put(key, value);
        if (currentMemtable.getRawSize() >= MEMTABLE_THRESHOLD) {
            createSSTable();
        }
    }

    @Override
    public String read(String key) {
        if (currentMemtable.containsKey(key))
            return currentMemtable.get(key);
        else if (flushingMemtable != null && flushingMemtable.containsKey(key))
            return flushingMemtable.get(key);
        else {
            for (InMemIndex indexCache : segmentInMemIndexList) {
                if (indexCache.contains(key)) {
                    return indexCache.get(key);
                }
            }
        }
        return null;
    }

    @Override
    public boolean delete(String key) {
        put(key, null);
    }

    @Override
    public boolean compareAndSet(String key, String oldValue, String newValue) {
        if (key == null)
            throw new IllegalArgumentException();
        if (oldValue == null)
            throw new IllegalArgumentException();
        if (newValue == null)
            throw new IllegalArgumentException();

        String value = read(key);
        if (!oldValue.equals(value))
            return false;
        put(key, newValue);
        return true;
    }

    @Override
    public void close() throws IOException {

    }

    private void createSSTable() throws IOException {
        this.flushingMemtable = this.currentMemtable;
        this.currentMemtable = new Memtable();

        Path dbPath = generateSSTablePath();
        SSTable ssTable = SSTable.flush(this.flushingMemtable, dbPath.toString());
        InMemIndex memIndex = new InMemIndex(ssTable);
        segmentInMemIndexList.add(0, memIndex);
        compactAndMerge();
    }

    private Path generateSSTablePath() {
        return Paths.get(".", generateSSTableName());
    }

    private String generateSSTableName() {
        return System.currentTimeMillis() + ".db";
    }

    private void compactAndMerge() {

    }
}

package org.ctp.core.storageengine.lsm;

import org.ctp.core.storageengine.IStorageEngine;

import java.io.IOException;

public class LsmStorageEngine implements IStorageEngine {
    @Override
    public void initEngine() {

    }

    @Override
    public boolean put(String key, String value) {
        return false;
    }

    @Override
    public String read(String key) {
        return null;
    }

    @Override
    public boolean delete(String key) {
        return false;
    }

    @Override
    public boolean compareAndSet(String key, String oldValue, String newValue) {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}

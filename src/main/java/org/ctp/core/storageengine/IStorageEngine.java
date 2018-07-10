package org.ctp.core.storageengine;

import java.io.Closeable;

public interface IStorageEngine extends Closeable {
    void initEngine();
    boolean put(String key, String value);
    String read(String key);
    boolean delete(String key);
    boolean compareAndSet(String key, String oldValue, String newValue);
}

package org.ctp.server.storageengine;

import org.ctp.server.configuration.ServerConfiguration;

import java.io.Closeable;

public interface StorageEngine extends Closeable {
    void start();
    boolean put(String key, String value);
    String read(String key);
    boolean delete(String key);
    boolean compareAndSet(String key, String oldValue, String newValue);
    String getDiagnosisInfo();
}

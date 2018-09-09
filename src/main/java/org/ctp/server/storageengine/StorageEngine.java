package org.ctp.server.storageengine;

import org.ctp.server.configuration.ServerConfiguration;
import org.ctp.server.storageengine.command.ResultHandler;

import java.io.Closeable;

public interface StorageEngine extends Closeable {
    void start();
    void put(String key, String value, ResultHandler resultHandler);
    void read(String key, ResultHandler resultHandler);
    void delete(String key, ResultHandler resultHandler);
    void compareAndSet(String key, String oldValue, String newValue, ResultHandler resultHandler);
    void getDiagnosisInfo(ResultHandler resultHandler);
    void flush(ResultHandler resultHandler);
}

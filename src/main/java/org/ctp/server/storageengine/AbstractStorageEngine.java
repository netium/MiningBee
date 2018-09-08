package org.ctp.server.storageengine;

import org.ctp.server.configuration.ServerConfiguration;

import java.io.IOException;

public abstract class AbstractStorageEngine implements StorageEngine {
    private ServerConfiguration serverConfiguration;

    public ServerConfiguration getServerConfiguration() {
        return serverConfiguration;
    }

    public AbstractStorageEngine(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    @Override
    public void close() throws IOException { }
}

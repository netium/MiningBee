package org.ctp.server.context;

import org.ctp.server.configuration.ServerConfiguration;
import org.ctp.server.storageengine.StorageEngine;

public final class ServerContext {
    private ServerConfiguration serverConfiguration;
    private StorageEngine storageEngine;

    public static ServerContextBuilder builder() {
        return new ServerContextBuilder();
    }

    public ServerConfiguration getServerConfiguration() {
        return serverConfiguration;
    }

    public StorageEngine getStorageEngine() {
        return storageEngine;
    }

    private ServerContext(ServerConfiguration serverConfiguration, StorageEngine storageEngine) {
        this.serverConfiguration = serverConfiguration;
        this.storageEngine = storageEngine;
    }

    public static class ServerContextBuilder {
        private ServerConfiguration serverConfiguration;
        private StorageEngine storageEngine;

        public ServerContextBuilder setServerConfiguration(ServerConfiguration serverConfiguration) {
            this.serverConfiguration = serverConfiguration;
            return this;
        }

        public ServerContextBuilder setStorageEngine(StorageEngine storageEngine) {
            this.storageEngine = storageEngine;
            return this;
        }

        public ServerContext build() {
            return new ServerContext(serverConfiguration, storageEngine);
        }

        private ServerContextBuilder() {}
    }
}

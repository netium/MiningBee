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

package org.netium.server.context;

import org.netium.server.configuration.ServerConfiguration;
import org.netium.server.storageengine.StorageEngine;

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

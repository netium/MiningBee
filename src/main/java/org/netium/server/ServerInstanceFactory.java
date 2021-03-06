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

package org.netium.server;

import org.netium.server.storageengine.StorageEngine;
import org.netium.server.cluster.raft.RaftBaseClusterServer;
import org.netium.server.configuration.ServerConfiguration;
import org.netium.server.single.TelnetBaseServer;

public final class ServerInstanceFactory {
    private ServerInstanceFactory() {}

    public static ZeusServer createServerInstance(final ServerConfiguration serverConfiguration, final StorageEngine storageEngine) throws Exception {
        if ("single".equals(serverConfiguration.getMode()))
            return createSingleServerInstance(serverConfiguration, storageEngine);
        else
            return createClusterServerInstance(serverConfiguration, storageEngine);
    }

    private static ZeusServer createClusterServerInstance(final ServerConfiguration serverConfiguration, StorageEngine storageEngine) throws Exception {
        ZeusServer server = new RaftBaseClusterServer(
                serverConfiguration,
                storageEngine);
        return server;
    }

    private static ZeusServer createSingleServerInstance(final ServerConfiguration serverConfiguration, StorageEngine storageEngine) {
        ZeusServer server = new TelnetBaseServer(
                Short.parseShort(serverConfiguration.getSingle().get("port")),
                storageEngine);
        return server;
    }

}

package org.ctp.server;

import org.ctp.server.storageengine.StorageEngine;
import org.ctp.server.cluster.raft.RaftBaseClusterServer;
import org.ctp.server.configuration.ServerConfiguration;
import org.ctp.server.single.TelnetBaseServer;

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

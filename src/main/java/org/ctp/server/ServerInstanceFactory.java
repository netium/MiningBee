package org.ctp.server;

import org.ctp.cli.AppCliParameters;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.server.cluster.raft.RaftBaseClusterServer;
import org.ctp.server.single.TelnetBaseServer;

public final class ServerInstanceFactory {
    private ServerInstanceFactory() {}

    public static ZeusServer createServerInstance(final AppCliParameters cliParameters, final IStorageEngine storageEngine) throws Exception {
        if (cliParameters.isSingle())
            return createSingleServerInstance(cliParameters, storageEngine);
        else
            return createClusterServerInstance(cliParameters, storageEngine);
    }

    private static ZeusServer createClusterServerInstance(AppCliParameters cliParameters, IStorageEngine storageEngine) throws Exception {
        ZeusServer server = new RaftBaseClusterServer(cliParameters.getConfigurationFile(), cliParameters.getServerId(), "zeus-cluster", storageEngine);
        return server;
    }

    private static ZeusServer createSingleServerInstance(AppCliParameters cliParameters, IStorageEngine storageEngine) {
        ZeusServer server = new TelnetBaseServer(18889, storageEngine);
        return server;
    }

}

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

package org.netium.server.cluster.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.netium.server.configuration.ServerConfiguration;
import org.netium.server.storageengine.StorageEngine;
import org.netium.server.ZeusServer;
import org.netium.network.telnet.RaftBaseServerInitializer;
import org.jgroups.JChannel;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftBaseClusterServer implements ZeusServer {
    private final Logger logger = LoggerFactory.getLogger(RaftBaseClusterServer.class);

    private StorageEngine storageEngine;

    private String clusterName;
    private String serverId;

    private JChannel channel;
    private RaftHandle raftHandle;
    private ZeusStateMachine stateMachine;

    private ServerConfiguration serverConfiguration;


    public RaftBaseClusterServer(final ServerConfiguration serverConfiguration, final StorageEngine storageEngine) throws Exception {
        this.serverConfiguration = serverConfiguration;

        final String configurationPath = serverConfiguration.getCluster().get("conf"),
        serverId = serverConfiguration.getCluster().get("serverId");
        clusterName = serverConfiguration.getCluster().get("clusterName");

        this.storageEngine = storageEngine;

        this.clusterName = clusterName;
        this.serverId = serverId;

        channel = new JChannel(configurationPath);
        stateMachine = new ZeusStateMachine(storageEngine);
        raftHandle = new RaftHandle(channel, stateMachine).raftId(serverId);
        raftHandle.addRoleListener(stateMachine);
    }

    public void start() throws Exception {
        logger.info("Connecting to cluster: " + clusterName);
        channel.connect(clusterName);

        runByNetty();
    }

    private void runByNetty() throws InterruptedException {
        final int PORT = Integer.parseInt(serverConfiguration.getCluster().get("port"));

        logger.info("Activate the netty on port: " + PORT);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new RaftBaseServerInitializer(raftHandle));

            b.bind(PORT).sync().channel().closeFuture().sync();
        }
        finally {
            if (bossGroup != null)
                bossGroup.shutdownGracefully();
            if (workerGroup != null)
                workerGroup.shutdownGracefully();
        }
    }
}

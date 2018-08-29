package org.ctp.core.storageengine.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.server.smr.ZeusStateMachine;
import org.ctp.network.telnet.RaftBaseServerInitializer;
import org.jgroups.JChannel;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftBaseClusterServer implements ZeusServer {
    private final Logger logger = LoggerFactory.getLogger(RaftBaseClusterServer.class);

    private IStorageEngine storageEngine;

    private String clusterName;
    private String serverId;

    private JChannel channel;
    private RaftHandle raftHandle;
    private ZeusStateMachine stateMachine;


    public RaftBaseClusterServer(final String configurationPath, final String serverId, final String clusterName, final IStorageEngine storageEngine) throws Exception {
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
        final int PORT = 18889;

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

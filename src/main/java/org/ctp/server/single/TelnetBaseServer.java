package org.ctp.server.single;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.server.ZeusServer;
import org.ctp.network.telnet.TelnetServerInitializer;

public class TelnetBaseServer implements ZeusServer {

    private IStorageEngine storageEngine;
    private int port;

    public TelnetBaseServer(final int port, final IStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
        this.port = port;
    }

    @Override
    public void start() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new TelnetServerInitializer(storageEngine));

            b.bind(port).sync().channel().closeFuture().sync();
        }
        finally {
            if (bossGroup != null)
                bossGroup.shutdownGracefully();
            if (workerGroup != null)
                workerGroup.shutdownGracefully();
        }
    }
}

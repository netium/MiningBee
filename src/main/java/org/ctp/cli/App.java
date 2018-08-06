package org.ctp.cli;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;
import org.ctp.network.telnet.TelnetServerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class App
{
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private IStorageEngine storageEngine;

    private Thread networkThread;

    public static void main( String[] args ) throws InterruptedException {
        App app = new App();
        app.init("./db");
        app.showBanner();
        // app.runCommandLoop();
        app.runByNetty();
    }

    private void showBanner() {
        System.out.println("Welcome to DistKV");
        showEngineDiagnosisInfo();
    }

    private void showEngineDiagnosisInfo() {
        System.out.println(storageEngine.getDiagnosisInfo());
    }

    private void init(String dbPath) {
        storageEngine = new LsmStorageEngine();
        storageEngine.initEngine(dbPath);
    }

    private void runCommandLoop()  {
        CliCommandLoop executor = new CliCommandLoop(storageEngine, System.in, System.out, System.err);
        executor.execute();
    }

    private void runByNetty() throws InterruptedException {
        final int PORT = 18889;
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new TelnetServerInitializer(storageEngine));

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

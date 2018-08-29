package org.ctp.cli;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.cli.*;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;
import org.ctp.core.storageengine.server.raft.RaftBaseClusterServer;
import org.ctp.network.telnet.TelnetServerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private IStorageEngine storageEngine;

    private Thread networkThread;

    public static void main( String[] args ) throws Exception {
        App app = new App();

        AppCliParameters appParams = app.parseCommandlines(args);

        app.init("./db");
        app.showBanner();
        // app.runCommandLoop();
        // app.runByNetty();
        app.runByJgroupsRaft(appParams);
    }

    private AppCliParameters parseCommandlines(String[] args) throws ParseException {
        Option configurationFileProperty = OptionBuilder.withArgName("file")
                .hasArg()
                .withDescription("The cluster configuration file")
                .create("conf");

        configurationFileProperty.setRequired(true);

        Option serverIdProperty = OptionBuilder.withArgName("serverId")
                .hasArg()
                .withDescription("The server ID")
                .create("serverId");

        serverIdProperty.setRequired(true);

        Options options = new Options();

        options.addOption(configurationFileProperty);
        options.addOption(serverIdProperty);


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        AppCliParameters appParams = new AppCliParameters();
        appParams.setConfigurationFile(cmd.getOptionValue("conf"));
        appParams.setServerId(cmd.getOptionValue("serverId"));

        return appParams;
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

    private void runByJgroupsRaft(AppCliParameters appParams) throws Exception {

        final String configuration = appParams.getConfigurationFile();
        final String clusterName = "zeus-cluster";
        RaftBaseClusterServer server = new RaftBaseClusterServer(configuration, appParams.getServerId(), clusterName, storageEngine);
        server.start();
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

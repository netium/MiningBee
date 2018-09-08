package org.ctp.cli;

import com.sun.security.ntlm.Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.cli.*;
import org.ctp.server.storageengine.StorageEngine;
import org.ctp.server.storageengine.lsm.LsmStorageEngine;
import org.ctp.server.ServerInstanceFactory;
import org.ctp.server.ZeusServer;
import org.ctp.network.telnet.TelnetServerInitializer;
import org.ctp.server.configuration.ServerConfiguration;
import org.ctp.server.configuration.ServerConfigurationLoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private StorageEngine storageEngine;

    private Thread networkThread;

    public static void main( String[] args ) throws Exception {
        App app = new App();

        AppCliParameters appParams = app.parseCommandlines(args);
        ServerConfiguration configuration = app.loadConfiguration(appParams);

        app.init(configuration);
        app.showBanner();
        // app.runCommandLoop();
        // app.runByNetty();

        app.run(configuration);
        // runByJgroupsRaft(appParams);
    }

    private ServerConfiguration loadConfiguration(AppCliParameters appParams) throws ServerConfigurationLoadException {
        return ServerConfiguration.loadFromFile(appParams.getConfigurationFile());
    }

    private void run(ServerConfiguration configuration) throws Exception {
        ZeusServer server = ServerInstanceFactory.createServerInstance(configuration, storageEngine);
        server.start();
    }

    private AppCliParameters parseCommandlines(String[] args) throws ParseException {
        Option configurationFileProperty = OptionBuilder.withArgName("file")
                .hasArg()
                .withDescription("The cluster configuration file")
                .create("conf");

        configurationFileProperty.setRequired(true);

        Options options = new Options();

        options.addOption(configurationFileProperty);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        AppCliParameters appParams = new AppCliParameters();
        appParams.setConfigurationFile(cmd.getOptionValue("conf"));

        return appParams;
    }

    private void showBanner() {
        System.out.println("Welcome to Zeus, a mini distributed key value storage");
        showEngineDiagnosisInfo();
    }

    private void showEngineDiagnosisInfo() {
        System.out.println(storageEngine.getDiagnosisInfo());
    }

    private void init(ServerConfiguration configuration) {
        storageEngine = new LsmStorageEngine();
        storageEngine.initEngine(configuration);
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

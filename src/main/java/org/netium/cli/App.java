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

package org.netium.cli;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.cli.*;
import org.netium.server.storageengine.StorageEngine;
import org.netium.server.storageengine.lsm.LsmStorageEngine;
import org.netium.server.ServerInstanceFactory;
import org.netium.server.ZeusServer;
import org.netium.network.telnet.TelnetServerInitializer;
import org.netium.server.configuration.ServerConfiguration;
import org.netium.server.configuration.ServerConfigurationLoadException;
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
        storageEngine.getDiagnosisInfo(p->System.out.println(p.getReturnValue()));
    }

    private void init(ServerConfiguration configuration) {
        storageEngine = new LsmStorageEngine(configuration);
        storageEngine.start();
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

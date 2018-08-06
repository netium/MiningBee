package org.ctp.cli;

import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;
import org.ctp.network.telnet.TelnetBaseNetworkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class App
{
    private final Logger logger = LoggerFactory.getLogger(App.class);

    private IStorageEngine storageEngine;

    private Thread networkThread;

    public static void main( String[] args ) {
        App app = new App();
        app.init("./db");
        app.showBanner();
        // app.runCommandLoop();
        app.runForNetwork();
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

    private void runForNetwork() {
        try {
            TelnetBaseNetworkServer server = new TelnetBaseNetworkServer(storageEngine);
            Thread thread = new Thread(server);
            thread.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package org.ctp;

import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;

import java.io.DataInputStream;
import java.util.Scanner;

public class App
{
    private IStorageEngine storageEngine;

    public static void main( String[] args ) {
        App app = new App();
        app.init("./db");
        app.showBanner();
        app.runCommandLoop();
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

    private void runCommandLoop() {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            executeCommand(line);
        }
    }

    private void executeCommand(String command) {
        String[] tokens = command.split(" ");
        boolean ret;
        switch (tokens[0]) {
            case "exit":
                System.exit(0);
                break;
            case "put":
                ret = storageEngine.put(tokens[1], tokens[2]);
                if (ret) System.out.println("Succeeded");
                else System.out.println("Failed");
                break;
            case "get":
                String value = storageEngine.read(tokens[1]);
                System.out.println("Value: " + value);
                break;
            case "delete":
                ret = storageEngine.delete(tokens[1]);
                if (ret) System.out.println("Succeeded");
                else System.out.println("Failed");
                break;
            case "flush":
                if (storageEngine instanceof LsmStorageEngine) {
                    LsmStorageEngine lsmEngine = (LsmStorageEngine)storageEngine;
                    ret = lsmEngine.flush();
                    if (ret) System.out.println("Succeeded");
                    else System.out.println("Failed");
                }
                break;
            case "engine":
                System.out.println(storageEngine.getDiagnosisInfo());
            default:
                System.out.println("Invalid command");
                break;
        }
    }
}

package org.ctp.cli;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.IOException;
import java.util.Scanner;

public class App
{
    private final Logger logger = LoggerFactory.getLogger(App.class);

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

    private void runCommandLoop()  {
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.append(">>");
            String line = in.nextLine() + "\n";
            cliLexer lexer = new cliLexer(new ANTLRInputStream(line));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            cliParser parser = new cliParser(tokens);
            ParseTree tree = parser.commands();
            ParseTreeWalker walker = new ParseTreeWalker();
            CliCommandExecutor commandExecutor = new CliCommandExecutor(storageEngine);
            walker.walk(commandExecutor, tree);
        }
    }
}

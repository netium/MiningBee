package org.ctp.network.telnet;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.ctp.cli.cliBaseListener;
import org.ctp.cli.cliLexer;
import org.ctp.cli.cliParser;
import org.ctp.server.storageengine.StorageEngine;
import org.ctp.server.storageengine.lsm.LsmStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Scanner;

public class TelnetCommandExecutor extends cliBaseListener {
    private final Logger logger = LoggerFactory.getLogger(TelnetCommandExecutor.class);

    private final StorageEngine storageEngine;

    private PrintStream outputPrintStream;
    private PrintStream errorPrintStream;

    private boolean clientWantExiting = false;

    public boolean isClientWantExiting() {
        return clientWantExiting;
    }

    public TelnetCommandExecutor(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    public void executeCommand(String command, PrintStream outputStream, PrintStream errorStream) {
        this.outputPrintStream = outputStream;
        this.errorPrintStream = errorStream;

        Scanner in = new Scanner(command);

            String line = in.nextLine() + "\n";
            cliLexer lexer = new cliLexer(new ANTLRInputStream(line));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            cliParser parser = new cliParser(tokens);
            ParseTree tree = parser.commands();
            ParseTreeWalker walker = new ParseTreeWalker();
            walker.walk(this, tree);
    }

    @Override
    public void exitPut_command(cliParser.Put_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.put(ctx.KEYSTRING().getText(), normalizeValueString(ctx.VALUESTRING().getText()));
        if (ret) outputPrintStream.println("Succeeded");
        else outputPrintStream.println("Failed");
    }

    @Override
    public void exitGet_command(cliParser.Get_commandContext ctx) {
        if (ctx.exception != null)
            return;
        String value = storageEngine.read(ctx.KEYSTRING().getText());
        outputPrintStream.println("Value: " + value);
    }

    @Override
    public void exitDelete_command(cliParser.Delete_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.delete(ctx.KEYSTRING().getText());
        if (ret) outputPrintStream.println("Succeeded");
        else outputPrintStream.println("Failed");
    }

    @Override
    public void exitCas_command(cliParser.Cas_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.compareAndSet(ctx.KEYSTRING().getText(), normalizeValueString(ctx.VALUESTRING(0).getText()), normalizeValueString(ctx.VALUESTRING(1).getText()));
        if (ret) outputPrintStream.println("Succeeded");
        else outputPrintStream.println("Failed");
    }

    @Override
    public void exitFlush_command(cliParser.Flush_commandContext ctx) {
        if (ctx.exception != null)
            return;
        if (storageEngine instanceof LsmStorageEngine) {
            LsmStorageEngine lsmEngine = (LsmStorageEngine) storageEngine;
            boolean ret = lsmEngine.flush();
            if (ret) outputPrintStream.println("Succeeded");
            else outputPrintStream.println("Failed");
        }
    }

    @Override
    public void exitEngine_command(cliParser.Engine_commandContext ctx) {
        if (ctx.exception != null)
            return;
        outputPrintStream.println(storageEngine.getDiagnosisInfo());
    }

    @Override
    public void exitExit_command(cliParser.Exit_commandContext ctx) {
        if (ctx.exception != null)
            return;

        outputPrintStream.println("bye!");
        clientWantExiting = true;
    }

    private String normalizeValueString(String valueString) {
        StringBuilder sb = new StringBuilder(valueString);
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(0);
        return sb.toString();
    }
}
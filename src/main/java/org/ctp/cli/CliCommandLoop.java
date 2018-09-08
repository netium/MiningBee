package org.ctp.cli;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.ctp.server.storageengine.StorageEngine;
import org.ctp.server.storageengine.lsm.LsmStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class CliCommandLoop {
    private final StorageEngine storageEngine;
    private final InputStream inputStream;
    private final PrintStream outputPrintStream;
    private final PrintStream errorPrintStream;

    public CliCommandLoop(StorageEngine storageEngine, InputStream inputStream, PrintStream printStream, PrintStream errorStream) {
        this.storageEngine= storageEngine;
        this.inputStream = inputStream;
        this.outputPrintStream = printStream;
        this.errorPrintStream = errorStream;
    }

    public void execute() {
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.append(">>");
            String line = in.nextLine() + "\n";
            cliLexer lexer = new cliLexer(new ANTLRInputStream(line));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            cliParser parser = new cliParser(tokens);
            ParseTree tree = parser.commands();
            ParseTreeWalker walker = new ParseTreeWalker();
            CommandExecutor commandExecutor = this.new CommandExecutor();
            walker.walk(commandExecutor, tree);
        }
    }

private class CommandExecutor extends cliBaseListener {
    private final Logger logger = LoggerFactory.getLogger(CommandExecutor.class);

    public CommandExecutor() {
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
        System.exit(0);
    }

    private String normalizeValueString(String valueString) {
        StringBuilder sb = new StringBuilder(valueString);
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(0);
        return sb.toString();
    }
}
}

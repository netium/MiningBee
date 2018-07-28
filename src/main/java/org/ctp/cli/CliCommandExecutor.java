package org.ctp.cli;

import org.antlr.v4.runtime.tree.ErrorNode;
import org.ctp.core.storageengine.IStorageEngine;
import org.ctp.core.storageengine.lsm.LsmStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CliCommandExecutor extends cliBaseListener {
    private final Logger logger = LoggerFactory.getLogger(CliCommandExecutor.class);
    private IStorageEngine storageEngine;
    public CliCommandExecutor(IStorageEngine storageEngine) {
        if (storageEngine == null)
            throw new IllegalArgumentException("The instance of the storage engine is null");

        this.storageEngine = storageEngine;
    }

    @Override
    public void exitPut_command(cliParser.Put_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.put(ctx.KEYSTRING().getText(), normalizeValueString(ctx.VALUESTRING().getText()));
        if (ret) System.out.println("Succeeded");
        else System.out.println("Failed");
    }

    @Override
    public void exitGet_command(cliParser.Get_commandContext ctx) {
        if (ctx.exception != null)
            return;
        String value = storageEngine.read(ctx.KEYSTRING().getText());
        System.out.println("Value: " + value);
    }

    @Override public void exitDelete_command(cliParser.Delete_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.delete(ctx.KEYSTRING().getText());
        if (ret) System.out.println("Succeeded");
        else System.out.println("Failed");
    }

    @Override public void exitCas_command(cliParser.Cas_commandContext ctx) {
        if (ctx.exception != null)
            return;
        boolean ret = storageEngine.compareAndSet(ctx.KEYSTRING().getText(), normalizeValueString(ctx.VALUESTRING(0).getText()), normalizeValueString(ctx.VALUESTRING(1).getText()));
        if (ret) System.out.println("Succeeded");
        else System.out.println("Failed");
    }

    @Override public void exitFlush_command(cliParser.Flush_commandContext ctx) {
        if (ctx.exception != null)
            return;
        if (storageEngine instanceof LsmStorageEngine) {
            LsmStorageEngine lsmEngine = (LsmStorageEngine)storageEngine;
            boolean ret = lsmEngine.flush();
            if (ret) System.out.println("Succeeded");
            else System.out.println("Failed");
        }
    }

    @Override public void exitEngine_command(cliParser.Engine_commandContext ctx) {
        if (ctx.exception != null)
            return;
        System.out.println(storageEngine.getDiagnosisInfo());
    }

    @Override public void exitExit_command(cliParser.Exit_commandContext ctx) {
        if (ctx.exception != null)
            return;
        System.out.println("bye!");
        System.exit(0);
    }

    private String normalizeValueString(String valueString) {
        StringBuilder sb = new StringBuilder(valueString);
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(0);
        return sb.toString();
    }
}

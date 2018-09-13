package org.ctp.server.storageengine.command;

public abstract class Command {
    private final ResultHandler resultHandler;

    public Command(ResultHandler resultHandler) {
        this.resultHandler = resultHandler;
    }

    public ResultHandler getResultHandler() {
        return resultHandler;
    }
}

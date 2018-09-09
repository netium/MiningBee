package org.ctp.server.storageengine.command;

public final class DeleteCommand extends KeyBaseCommand {
    public DeleteCommand(String key, ResultHandler resultHandler) {
        super(key, resultHandler);
    }
}

package org.ctp.server.storageengine.command;

public final class GetCommand extends KeyBaseCommand {
    public GetCommand(String key, ResultHandler resultHandler) {
        super(key, resultHandler);
    }
}

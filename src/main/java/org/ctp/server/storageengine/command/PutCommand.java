package org.ctp.server.storageengine.command;

public final class PutCommand extends KeyBaseCommand {
    private final String value;

    public PutCommand(String key, String value, ResultHandler resultHandler) {
        super(key, resultHandler);
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

package org.ctp.server.storageengine.command;

public final class CompareAndSetCommand extends KeyBaseCommand {
    private final String oldValue;
    private final String newValue;

    public CompareAndSetCommand(String key, String oldValue, String newValue, ResultHandler resultHandler) {
        super(key, resultHandler);
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public String getOldValue() {
        return oldValue;
    }

    public String getNewValue() {
        return newValue;
    }
}

package org.ctp.server.storageengine.command;

public abstract class KeyBaseCommand extends Command {
    private final String key;

    public KeyBaseCommand(String key, ResultHandler resultHandler) {
        super(resultHandler);

        this.key = key;
    }

    public String getKey() {
        return key;
    }
}

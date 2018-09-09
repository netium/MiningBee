package org.ctp.server.storageengine.command;

@FunctionalInterface
public interface ResultHandler {
    void handle(CommandResult result);
}

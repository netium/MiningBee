package org.ctp.server.storageengine.command;

public class CommandResult {
    private final ResultStatus status;
    private final Throwable errorCause;
    private final String returnValue;

    public CommandResult(ResultStatus status) {
        this(status, null, null);
    }

    public CommandResult(ResultStatus status, String value) {
        this(status, value, null);
    }

    public CommandResult(ResultStatus status, String value, Throwable errorCause) {
        this.status = status;
        this.returnValue = value;
        this.errorCause = errorCause;
    }

    public ResultStatus getStatus() {
        return status;
    }

    public String getReturnValue() {
        return returnValue;
    }

    public Throwable getErrorCause() {
        return errorCause;
    }
}

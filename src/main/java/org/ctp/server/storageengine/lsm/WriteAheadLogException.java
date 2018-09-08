package org.ctp.server.storageengine.lsm;

public class WriteAheadLogException extends RuntimeException {
    public WriteAheadLogException() {
        super();
    }

    public WriteAheadLogException(String message) {
        super(message);
    }

    public WriteAheadLogException(String message, Throwable cause) {
        super(message, cause);
    }

    public WriteAheadLogException(Throwable cause) {
        super(cause);
    }
}

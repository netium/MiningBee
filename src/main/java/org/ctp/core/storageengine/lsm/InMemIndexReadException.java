package org.ctp.core.storageengine.lsm;

public class InMemIndexReadException extends RuntimeException {
    static final long serialVersionUID = -7034897190745766939L;

    public InMemIndexReadException() {
        super();
    }

    public InMemIndexReadException(String message) {
        super(message);
    }

    public InMemIndexReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public InMemIndexReadException(Throwable cause) {
        super(cause);
    }
}

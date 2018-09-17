package org.ctp.server.storageengine.lsm.sstable;

public class BadSSTableException extends RuntimeException {
    static final long serialVersionUID = -7034897190745766939L;

    public BadSSTableException() {
        super();
    }

    public BadSSTableException(String message) {
        super(message);
    }

    public BadSSTableException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadSSTableException(Throwable cause) {
        super(cause);
    }
}

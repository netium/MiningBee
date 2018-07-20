package org.ctp.core.storageengine.lsm;

public class LsmStorageEngineException extends RuntimeException {
    static final long serialVersionUID = -7034897190745766939L;

    public LsmStorageEngineException() {
        super();
    }

    public LsmStorageEngineException(String message) {
        super(message);
    }

    public LsmStorageEngineException(String message, Throwable cause) {
        super(message, cause);
    }

    public LsmStorageEngineException(Throwable cause) {
        super(cause);
    }
}

package org.ctp.server.storageengine.lsm;

public class AcquireLockTimeoutException extends RuntimeException {
        public AcquireLockTimeoutException() { super(); }

        public AcquireLockTimeoutException(String message) { super(message); }

        public AcquireLockTimeoutException(String message, Throwable cause) { super(message, cause); }

        public AcquireLockTimeoutException(Throwable cause) { super(cause); }
}

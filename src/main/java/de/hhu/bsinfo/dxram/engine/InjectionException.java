package de.hhu.bsinfo.dxram.engine;

public class InjectionException extends RuntimeException {

    public InjectionException() {
    }

    public InjectionException(String message) {
        super(message);
    }

    public InjectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InjectionException(Throwable cause) {
        super(cause);
    }

    public InjectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

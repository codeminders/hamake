package com.codeminders.hamake;

public class PigNotFoundException extends Exception {
    public PigNotFoundException() {
    }

    public PigNotFoundException(String message) {
        super(message);
    }

    public PigNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public PigNotFoundException(Throwable cause) {
        super(cause);
    }
}

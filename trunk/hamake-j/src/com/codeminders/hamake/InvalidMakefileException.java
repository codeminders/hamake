package com.codeminders.hamake;

public class InvalidMakefileException extends Exception {
    
    InvalidMakefileException() {
        super();
    }

    InvalidMakefileException(String message) {
        super(message);
    }
}

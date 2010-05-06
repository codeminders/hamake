package com.codeminders.hamake.syntax;

public class InvalidMakefileException extends Exception {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = -4257733876211235916L;

	public InvalidMakefileException() {
        super();
    }

    public InvalidMakefileException(String message) {
        super(message);
    }
}

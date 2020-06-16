package org.jdurani.rollingfile.exception;

/**
 * Exception indicating error while reading data.
 */
public class ReadException extends RuntimeException {

    /**
     * New instance.
     *
     * @param message message
     */
    public ReadException(String message) {
        super(message);
    }

    /**
     * New instance.
     *
     * @param message message
     * @param cause cause
     */
    public ReadException(String message, Throwable cause) {
        super(message, cause);
    }
}

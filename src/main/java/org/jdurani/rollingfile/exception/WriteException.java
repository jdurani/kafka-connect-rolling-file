package org.jdurani.rollingfile.exception;

/**
 * Exception indicating error while writing data.
 */
public class WriteException extends RuntimeException {

    /**
     * New instance.
     *
     * @param message message
     * @param cause cuase
     */
    public WriteException(String message, Throwable cause) {
        super(message, cause);
    }
}

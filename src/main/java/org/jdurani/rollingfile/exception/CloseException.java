package org.jdurani.rollingfile.exception;

/**
 * Exception indicating error while closing file.
 */
public class CloseException extends RuntimeException {

    /**
     * New instance.
     *
     * @param message message
     */
    public CloseException(String message) {
        super(message);
    }
}

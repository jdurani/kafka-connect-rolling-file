package org.jdurani.rollingfile.exception;

/**
 * Exception indicating error while flushing data.
 */
public class FlushException extends RuntimeException {

    /**
     * New instance.
     *
     * @param message message
     */
    public FlushException(String message) {
        super(message);
    }
}

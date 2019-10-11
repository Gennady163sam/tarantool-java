package org.tarantool;

/**
 * Describes a client side error usually unrelated to
 * a Tarantool server.
 */
public class TarantoolClientException extends RuntimeException {

    public TarantoolClientException() {
    }

    public TarantoolClientException(String message) {
        super(message);
    }

    public TarantoolClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public TarantoolClientException(Throwable cause) {
        super(cause);
    }

}

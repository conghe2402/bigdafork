package com.opens.bigdafork.utils.common.exceptions;

import java.io.Serializable;

/**
 * To throw this exception when preparing context information
 * for authentication before making connections.
 */
public class InvalidLoginInfoException extends RuntimeException implements Serializable {
    private static final String MESSAGE_HEAD = "Error happenped while authenticating";

    public InvalidLoginInfoException() {
        this("");
    }

    public InvalidLoginInfoException(String msg) {
        super(String.format("%s : %s", MESSAGE_HEAD, msg));
    }
}

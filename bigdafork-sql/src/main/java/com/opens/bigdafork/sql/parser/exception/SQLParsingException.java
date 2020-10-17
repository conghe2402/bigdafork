package com.opens.bigdafork.sql.parser.exception;

import java.io.Serializable;

/**
 * SQLParsingException.
 */
public class SQLParsingException extends RuntimeException implements Serializable {
    private static final String MESSAGE_HEAD = "SQL Parse error：";

    public SQLParsingException() {
        this("");
    }

    public SQLParsingException(String msg) {
        super(String.format("%s : %s", MESSAGE_HEAD, msg));
    }
}

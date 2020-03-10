package com.opens.bigdafork.datatask.exception;

import java.io.Serializable;

/**
 * TaskFailException.
 * such as timeout and fail.
 */
public class TaskFailException extends RuntimeException implements Serializable {
    private static final String MESSAGE_HEAD = "Error happenped";

    public TaskFailException() {
        this("");
    }

    public TaskFailException(String msg) {
        super(String.format("%s : %s", MESSAGE_HEAD, msg));
    }
}

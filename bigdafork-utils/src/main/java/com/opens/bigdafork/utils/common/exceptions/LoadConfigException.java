package com.opens.bigdafork.utils.common.exceptions;

import java.io.Serializable;

/**
 * To throw this exception when loading configurations.
 */
public class LoadConfigException extends RuntimeException implements Serializable {

    private static final String MESSAGE_HEAD = "Error happenped while loading configuration file";

    public LoadConfigException() {
        this("");
    }

    public LoadConfigException(String msg) {
        super(String.format("%s : %s", MESSAGE_HEAD, msg));
    }
}

package com.opens.bigdafork.common.base.observe;

import lombok.Data;

/**
 * NotifyEvent.
 */
@Data
public class NotifyEvent {
    private boolean status;
    private String msg;
    private Object payload;
}

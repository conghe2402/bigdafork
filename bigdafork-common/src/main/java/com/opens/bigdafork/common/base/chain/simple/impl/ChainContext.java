package com.opens.bigdafork.common.base.chain.simple.impl;

import java.util.HashMap;

/**
 * ChainContext.
 */
public class ChainContext extends HashMap<String, Object> {

    private boolean isStop = false;

    public void next(boolean goOn) {
        this.isStop = !goOn;
    }

    public boolean getStop() {
        return this.isStop;
    }

    public static final String RESULT_VALUE = "result_value";


}

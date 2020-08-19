package com.opens.bigdafork.common.base.chain.simple.impl;

import java.util.HashMap;

/**
 * ChainContext.
 */
public class ChainContext extends HashMap<String, Object> {

    private boolean isStop = false;

    private boolean exitSys = false;

    public void next(boolean goOn) {
        this.isStop = !goOn;
    }

    public boolean getStop() {
        return this.isStop;
    }

    public boolean isExitSys() {
        return exitSys;
    }

    public void setExitSys(boolean exitSys) {
        this.exitSys = exitSys;
    }

    public static final String RESULT_VALUE = "result_value";


}

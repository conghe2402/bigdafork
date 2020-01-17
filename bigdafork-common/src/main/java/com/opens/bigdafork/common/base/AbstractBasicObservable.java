package com.opens.bigdafork.common.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Observer;

/**
 * add notification functionality via observable pattern.
 */
public abstract class AbstractBasicObservable extends Observable {

    private Observer observer;

    public AbstractBasicObservable() {
        this(null);
    }

    public AbstractBasicObservable(Observer observer) {
        this.observer = observer;
        if (this.observer == null) {
            this.setDefaultOb();
        }
    }

    /**
     * The behaviour of default ob is to shutdown app.
     * Its msg type is boolean type,
     * if true it is ok with nothing to do,
     * else[false] app will shutdown immediately.
     */
    public void setDefaultOb() {
        this.observer = new Observer() {
            private final Logger obLogger = LoggerFactory.getLogger(this.getClass());
            @Override
            public void update(Observable o, Object arg) {
                boolean status = (boolean) arg;
                if (!status) {
                    obLogger.debug("operate failure , then program will shutdown");
                    System.exit(0);
                } else {
                    obLogger.debug("operate success");
                }
            }
        };
        this.addObserver(this.observer);
    }

    /**
     * Unify notify method.
     * @param notifyMsg
     */
    protected void notify(Object notifyMsg) {
        this.setChanged();
        this.notifyObservers(notifyMsg);
    }
}

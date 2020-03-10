package com.opens.bigdafork.common.base.config.props;

import com.opens.bigdafork.common.base.observe.AbstractBasicObservable;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import com.opens.bigdafork.common.base.observe.AbstractBasicObserver;
import com.opens.bigdafork.common.base.observe.NotifyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a config load abstract utility.
 */
public abstract class AbstractPropertiesConfig<C extends AbstractPropertiesConfigLoader>
        extends AbstractBasicObservable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPropertiesConfig.class);

    private C configs = null;
    private boolean status = false;

    public AbstractPropertiesConfig() {
        this(false);
    }

    public AbstractPropertiesConfig(boolean stopWhenFail) {
        this(stopWhenFail, null, true);
    }

    public AbstractPropertiesConfig(AbstractBasicObserver observer, boolean init) {
        this(false, observer, init);
    }

    public AbstractPropertiesConfig(boolean stopWhenFail, AbstractBasicObserver observer,
                                    boolean init) {
        super(stopWhenFail, observer);
        if (init) {
            this.init();
        }
    }

    public String getConfig(String keyName) {
        if (status) {
            return this.configs.getProperty(keyName);
        } else {
            return "";
        }
    }

    public String getConfig(String keyName, String defaultValue) {
        if (status) {
            return this.configs.getProperty(keyName, defaultValue);
        } else {
            return defaultValue;
        }
    }

    public int getConfig(String keyName, int defaultValue) {
        return Integer.parseInt(getConfig(keyName, String.valueOf(defaultValue)));
    }

    public List<String> getAllConfigList() {
        if (status) {
            return this.configs.getAllConfigItemsAsList();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Just create instance of config properties.
     */
    protected abstract C newConfigProps() throws LoadConfigException;

    /**
     * To override this meth for extend your notify message.
     * @return
     */
    protected Object makePayload(boolean loadSuc, String loadMsg) {
        return null;
    }

    protected void init() {
        NotifyEvent event = new NotifyEvent();
        Object payload;
        try {
            configs = newConfigProps();
            status = true;
            event.setStatus(status);
            event.setMsg("ok");
            payload = makePayload(status, "ok");
        } catch (LoadConfigException e) {
            status = false;
            LOGGER.error(e.getMessage());
            event.setStatus(status);
            event.setMsg(e.getMessage());
            payload = makePayload(status, e.getMessage());
        }
        event.setPayload(payload);
        notify(event);
    }



}

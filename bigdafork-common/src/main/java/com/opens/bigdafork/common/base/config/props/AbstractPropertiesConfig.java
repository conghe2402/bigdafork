package com.opens.bigdafork.common.base.config.props;

import com.opens.bigdafork.common.base.observe.AbstractBasicObservable;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import com.opens.bigdafork.common.base.observe.AbstractBasicObserver;
import com.opens.bigdafork.common.base.observe.NotifyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is a config load abstract utility.
 */
public abstract class AbstractPropertiesConfig<C extends AbstractPropertiesConfigLoader>
        extends AbstractBasicObservable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPropertiesConfig.class);

    private Properties props = null;
    private C configsLoader = null;
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
            return this.getProperty(keyName);
        } else {
            return "";
        }
    }

    public String getConfig(String keyName, String defaultValue) {
        if (status) {
            return this.getProperty(keyName, defaultValue);
        } else {
            return defaultValue;
        }
    }

    public int getConfig(String keyName, int defaultValue) {
        return Integer.parseInt(getConfig(keyName, String.valueOf(defaultValue)));
    }

    public List<String> getAllConfigList() {
        if (status) {
            return this.getAllConfigItemsAsList();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Just create instance of config properties.
     */
    protected abstract C newConfigsLoader() throws LoadConfigException;

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
            configsLoader = newConfigsLoader();
            this.props = configsLoader.load();
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

    private String getProperty(String keyName) {
        return this.getProperty(keyName, "");
    }

    private String getProperty(String keyName, String defaultValue) {
        return this.props.getProperty(keyName, defaultValue);
    }

    private List<String> getAllConfigItemsAsList() {
        List<String> items = new ArrayList<>();
        for (Map.Entry item : props.entrySet()) {
            String key = (String)item.getKey();
            String value = (String)item.getValue();
            // TODO: 2020/2/13 value include "
            StringBuilder itemBuilder = new StringBuilder(key);
            itemBuilder.append("=").append(value);
            items.add(itemBuilder.toString());
        }
        return items;
    }
}

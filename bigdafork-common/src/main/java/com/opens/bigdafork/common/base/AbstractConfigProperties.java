package com.opens.bigdafork.common.base;

import com.opens.bigdafork.common.base.exception.LoadConfigException;
import lombok.Setter;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Abstract config properties.
 */
public abstract class AbstractConfigProperties implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigProperties.class);
    private Properties properties;

    @Setter
    private boolean loadSuccess = false;

    /**
     * custom load logic by yourself.
     * @throws LoadConfigException
     */
    public abstract void initialize() throws LoadConfigException;

    /**
     * Try to load properties specified by user, return false when it fails.
     * @param path
     * @return
     */
    public boolean loadExternalConfig(String path) {
        LOGGER.debug("try to load properties file from jvm variable");
        boolean isSuccess = false;
        try (InputStream inputStream = new FileInputStream(path)) {
            isSuccess = loadConfigInputStream(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isSuccess;
    }

    public String getProperty(String keyName) {
        return this.getProperty(keyName, "");
    }

    public String getProperty(String keyName, String defaultValue) {
        return this.properties.getProperty(keyName, defaultValue);
    }

    public boolean isLoadSuccess() {
        return this.loadSuccess;
    }

    /**
     * properties loads source.
     * @param is
     * @return
     */
    protected boolean loadConfigInputStream(InputStream is) {
        boolean isSuccess = false;
        try (InputStreamReader reader = new InputStreamReader(is,
                Charsets.UTF_8)) {
            properties.load(reader);
            isSuccess = true;
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }
        return isSuccess;
    }
}

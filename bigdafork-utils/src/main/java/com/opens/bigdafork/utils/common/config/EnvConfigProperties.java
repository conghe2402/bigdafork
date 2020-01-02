package com.opens.bigdafork.utils.common.config;

import com.opens.bigdafork.utils.common.exceptions.LoadConfigException;
import com.opens.bigdafork.utils.common.util.FastjsonUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.Properties;

import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.CLASSPATH_PROPERTIES_FILE_NAME;
import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.JVM_PROPERTIES_PATH;
import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.JVM_CONTEXT_PROPERTIES;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * initialize context environment of application.
 */
public class EnvConfigProperties implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvConfigProperties.class);

    private Properties properties;

    private boolean loadSuccess = false;

    public EnvConfigProperties() throws LoadConfigException {
        initialize();
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
     * 1.read from jvm system vars by the key #JVM_CONTEXT_PROPERTIES.
     * 2.read file in path specified by jvm system vars.
     * 3.read file in classpath root.
     *
     * @throws LoadConfigException
     */
    private void initialize() throws LoadConfigException {
        if (this.isLoadSuccess()) {
            return;
        }
        LOGGER.debug("initialize cluster environment...");
        properties = new Properties();

        String contextPropertiesJson = System.getProperty(JVM_CONTEXT_PROPERTIES);
        if (StringUtils.isNotBlank(contextPropertiesJson)) {
            LOGGER.debug("try to load properties file from properties json");
            LOGGER.debug(contextPropertiesJson);
            Properties propsFromJson = FastjsonUtils.convertJSONToObject(contextPropertiesJson, Properties.class);
            if (propsFromJson != null) {
                properties.putAll(propsFromJson);
                loadSuccess = true;
            }
        }

        String path = System.getProperty(JVM_PROPERTIES_PATH);
        if (!loadSuccess && StringUtils.isNotBlank(path)) {
            loadSuccess = loadExternalConfig(path);
        }

        if (!loadSuccess) {
            loadSuccess = loadClassPathConfig();
        }

        if (!loadSuccess) {
            throw new LoadConfigException();
        }
    }

    /**
     * Try to load properties specified by user, return false when it fails.
     * @param path
     * @return
     */
    private boolean loadExternalConfig(String path) {
        LOGGER.debug("try to load properties file from jvm variable");
        boolean isSuccess = false;
        try (InputStream inputStream = new FileInputStream(path)) {
            isSuccess = loadConfig(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isSuccess;
    }

    /**
     * Try to load properties in inner classpath root,
     * if the file does not exist in classpath root directory, this method will return false.
     * @return
     */
    private boolean loadRootClassPathConfig() {
        LOGGER.debug("try to load properties file in root classpath root");
        return loadConfig(EnvConfigProperties.class
                .getClassLoader().getResourceAsStream(CLASSPATH_PROPERTIES_FILE_NAME));
    }

    /**
     * try to load properties in inner classpath root,
     * if the file does not exist in classpath root directory,
     * this method will return false.
     * @return
     */
    private boolean loadClassPathConfig() {
        LOGGER.debug("try to load properties file in classpath root");
        return loadConfig(EnvConfigProperties.class
                .getClassLoader().getResourceAsStream(CLASSPATH_PROPERTIES_FILE_NAME));
    }

    /**
     * properties loads source.
     * @param is
     * @return
     */
    private boolean loadConfig(InputStream is) {
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

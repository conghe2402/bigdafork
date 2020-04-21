package com.opens.bigdafork.utils.common.config;

import com.opens.bigdafork.common.base.config.props.AbstractPropertiesConfigLoader;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import com.opens.bigdafork.utils.common.util.FastjsonUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.CLASSPATH_PROPERTIES_FILE_NAME;
import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.JVM_PROPERTIES_PATH;
import static com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants.JVM_CONTEXT_PROPERTIES;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * initialize context environment of application.
 */
public class EnvPropertiesConfigLoader extends AbstractPropertiesConfigLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnvPropertiesConfigLoader.class);

    public EnvPropertiesConfigLoader() throws LoadConfigException {

    }

    /**
     * 1.read from jvm system vars by the key #JVM_CONTEXT_PROPERTIES.
     * 2.read file in path specified by jvm system vars.
     * 3.read file in classpath root.
     *
     * @throws LoadConfigException
     */
    @Override
    public Properties load() throws LoadConfigException {
        if (this.isLoadSuccess()) {
            return this.getProperties();
        }

        LOGGER.debug("initialize cluster environment...");

        String contextPropertiesJson = System.getProperty(JVM_CONTEXT_PROPERTIES);
        if (StringUtils.isNotBlank(contextPropertiesJson)) {
            LOGGER.debug("try to load properties file from properties json");
            LOGGER.debug(contextPropertiesJson);
            Properties propsFromJson = FastjsonUtils.convertJSONToObject(contextPropertiesJson, Properties.class);
            loadProperties(propsFromJson);
        }

        String path = System.getProperty(JVM_PROPERTIES_PATH);
        if (!this.isLoadSuccess() && StringUtils.isNotBlank(path)) {
            this.setLoadSuccess(loadExternalConfig(path));
        }
        if (!this.isLoadSuccess()) {
            this.setLoadSuccess(loadConfigFromClasspathRoot(CLASSPATH_PROPERTIES_FILE_NAME));
        }

        if (!this.isLoadSuccess()) {
            throw new LoadConfigException();
        }

        return this.getProperties();
    }
}

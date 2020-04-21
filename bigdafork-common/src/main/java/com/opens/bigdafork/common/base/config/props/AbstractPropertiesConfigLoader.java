package com.opens.bigdafork.common.base.config.props;

import com.opens.bigdafork.common.base.exception.LoadConfigException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Abstract config properties loader.
 */
public abstract class AbstractPropertiesConfigLoader implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPropertiesConfigLoader.class);

    @Getter
    private Properties properties = new Properties();

    @Setter
    private boolean loadSuccess = false;

    /**
     * custom load logic by yourself.
     * @throws LoadConfigException
     */
    public abstract Properties load() throws LoadConfigException;

    /**
     * Try to load properties specified by user, return false when it fails.
     * @param path
     * @return
     */
    public boolean loadExternalConfig(String path) {
        LOGGER.debug("try to load properties file specified by jvm variable");
        boolean isSuccess = false;
        try (InputStream inputStream = new FileInputStream(path)) {
            isSuccess = loadConfigInputStream(inputStream);
        } catch (IOException e) {
            LOGGER.error("path load fail : " + e.getMessage());
            //e.printStackTrace();
        }
        if (isSuccess) {
            LOGGER.debug(String.format("file %s load success ", path));
        } else {
            LOGGER.warn(String.format("file load fail, %s does not exist on FileSystem ", path));
        }
        return isSuccess;
    }

    public boolean isLoadSuccess() {
        return this.loadSuccess;
    }

    /**
     * Try to load properties in inner classpath root,
     * if the file does not exist in classpath root directory, this method will return false.
     * @param configFileName
     * @return
     */
    public boolean loadConfigFromClasspathRoot(String configFileName) {
        LOGGER.debug("try to load properties file in root classpath root");
        boolean result = false;
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(configFileName);
            if (is != null) {
                result = loadConfigInputStream(is);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (result) {
            LOGGER.debug(String.format("file %s load success ", configFileName));
        } else {
            LOGGER.warn(String.format("file load fail, %s does not exist in CLASS ROOT PATH", configFileName));
        }
        return result;
    }

    /**
     * properties loads source.
     * @param is
     * @return
     */
    public boolean loadConfigInputStream(InputStream is) {
        boolean isSuccess = false;
        if (is == null) {
            LOGGER.warn("can not load Config from input stream, is = null");
            return isSuccess;
        }
        try (InputStreamReader reader = new InputStreamReader(is,
                Charsets.UTF_8)) {
            properties.load(reader);
            isSuccess = true;

        } catch (IOException e) {
            e.printStackTrace();
        }
        if (isSuccess) {
            LOGGER.debug("load success from is");
        } else {
            LOGGER.warn("load fail from is");
        }
        return isSuccess;
    }

    public void loadProperties(Properties props) {
        if (props != null) {
            properties.putAll(props);
            this.setLoadSuccess(true);
            LOGGER.debug("load success from properties");
        } else {
            this.setLoadSuccess(false);
            LOGGER.debug("load fail from properties");
        }
    }

    /**
     * load way.
     */
    public enum LoadType {
        CP_ROOT, FILE_PATH, PROPS
    }
}

package com.opens.bigdafork.datatask.config;

import com.opens.bigdafork.common.base.config.props.AbstractPropertiesConfigLoader;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * initialize context environment of application.
 */
public class TaskPropertiesConfigLoader extends AbstractPropertiesConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskPropertiesConfigLoader.class);
    private String fileName;

    public TaskPropertiesConfigLoader(String file) {
        this.fileName = file;
    }

    /**
     * load from root path > load from external file.
     * @throws LoadConfigException
     */
    @Override
    public void load() throws LoadConfigException {
        if (!this.isLoadSuccess()) {
            setLoadSuccess(loadConfigFromClasspathRoot(fileName));
        }

        if (!this.isLoadSuccess()) {
            setLoadSuccess(loadExternalConfig(fileName));
        }

        if (!this.isLoadSuccess()) {
            throw new LoadConfigException(String.format("%s load fail", fileName));
        }

        LOGGER.debug(String.format("%s load success", fileName));
    }
}

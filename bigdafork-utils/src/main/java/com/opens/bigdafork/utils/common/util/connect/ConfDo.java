package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.utils.common.basic.IDo;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstraction what and how to do for configuration.
 */
public abstract class ConfDo implements IDo<Configuration, Configuration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfDo.class);

    @Override
    public Configuration iDo(Configuration entries) {
        LOGGER.debug("client init conf");
        Configuration config = entries;
        if (config == null) {
            config = new Configuration();
        }
        return this.prepareConf(config);
    }

    public abstract Configuration prepareConf(Configuration configuration);
}

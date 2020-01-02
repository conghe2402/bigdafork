package com.opens.bigdafork.utils.common.util.connect;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To define how to load configurations of hive.
 */
public class HiveConfDo extends ConfDo {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveConfDo.class);

    @Override
    public Configuration prepareConf(Configuration configuration) {
        LOGGER.debug("Hive conf initialize");
        return configuration;
    }
}

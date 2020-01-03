package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.utils.common.config.EnvConfigsLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        LOGGER.debug("load core-site file : " + env.getCoreSiteXml());
        configuration.addResource(new Path(env.getCoreSiteXml()));
        return configuration;
    }
}

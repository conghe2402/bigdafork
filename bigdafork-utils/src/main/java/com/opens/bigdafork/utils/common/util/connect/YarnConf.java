package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.utils.common.config.EnvPropertiesConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YarnConf.
 */
public class YarnConf extends ConfDo {
    private static final Logger LOGGER = LoggerFactory.getLogger(YarnConf.class);

    @Override
    public Configuration prepareConf(Configuration configuration) {
        LOGGER.debug("Yarn conf initialize");
        EnvPropertiesConfig env = EnvPropertiesConfig.getInstance();
        configuration.addResource(new Path(env.getCoreSiteXml()));
        configuration.addResource(new Path(env.getYarnSiteXml()));
        return configuration;
    }
}

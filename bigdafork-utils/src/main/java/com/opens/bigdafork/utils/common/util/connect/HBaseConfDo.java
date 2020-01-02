package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.utils.common.config.EnvConfigsLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To define how to load configurations of hbase.
 */
public class HBaseConfDo extends ConfDo {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConfDo.class);

    @Override
    public Configuration prepareConf(Configuration configuration) {
        LOGGER.debug("HBase conf initialize");
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        configuration.addResource(new Path(env.getCoreSiteXml()));
        configuration.addResource(new Path(env.getHBaseSiteXml()));
        configuration.addResource(new Path(env.getHDFSSiteXml()));
        return configuration;
    }
}

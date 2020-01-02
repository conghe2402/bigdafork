package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.utils.common.basic.IDo;
import com.opens.bigdafork.utils.common.config.EnvConfigsLoader;
import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a hive url setter module.
 * Default implementation is in zookeeper mode.
 */
public class HiveUrlDo implements IDo<Configuration, Configuration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUrlDo.class);
    private static final String HIVE_JDBC_URL = "jdbc:hive2://%s/"
            + ";serviceDiscoveryMode=zooKeeper"
            + ";zooKeeperNamespace=hiveserver2"
            + ";sasl.qop=auth-conf"
            + ";auth=KERBEROS"
            + ";principal=%s" + ";";

    @Override
    public Configuration iDo(Configuration configuration) {
        LOGGER.debug("Hive set url...");
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        String url = StringUtils.format(HIVE_JDBC_URL,
                env.getZookeeperQuorum(),
                env.getHivePrincipal());
        configuration.set(BigdataUtilsGlobalConstants.HIEV_JDBC_URL_KEY,
                url);
        return configuration;
    }
}

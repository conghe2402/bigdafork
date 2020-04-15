package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.common.base.IDo;
import com.opens.bigdafork.utils.common.config.EnvPropertiesConfig;
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
    private static final String HIVE_JDBC_URL_SAFE = new StringBuilder("jdbc:hive2://%s/")
            .append(";serviceDiscoveryMode=zooKeeper")
            .append(";zooKeeperNamespace=hiveserver2")
            .append(";sasl.qop=auth-conf")
            .append(";auth=KERBEROS")
            .append(";principal=%s;").toString();
    private static final String HIVE_JDBC_URL_NORMAL = new StringBuilder("jdbc:hive2://%s/")
            .append(";serviceDiscoveryMode=zooKeeper")
            .append(";zooKeeperNamespace=hiveserver2")
            .append(";auth=none").toString();

    private boolean isSafeMode = false;

    public HiveUrlDo(boolean isSafeMode) {
        this.isSafeMode = isSafeMode;
    }

    @Override
    public Configuration iDo(Configuration configuration) {
        LOGGER.debug("Hive set url...");
        EnvPropertiesConfig env = EnvPropertiesConfig.getInstance();

        String url = isSafeMode ?
                StringUtils.format(HIVE_JDBC_URL_SAFE,
                        env.getZookeeperQuorum(),
                        env.getHivePrincipal())
                :
                StringUtils.format(HIVE_JDBC_URL_NORMAL,
                        env.getZookeeperQuorum());
        configuration.set(BigdataUtilsGlobalConstants.HIEV_JDBC_URL_KEY,
                url);

        if (!isSafeMode) {
            configuration.set(BigdataUtilsGlobalConstants.USERNAME_NORMAL_MODE,
                    env.getUserNameNormalMode());
        }
        return configuration;
    }
}

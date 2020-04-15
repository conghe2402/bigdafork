package com.opens.bigdafork.utils.common.config;

import com.opens.bigdafork.common.base.config.props.AbstractPropertiesConfig;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;

/**
 * This is a config load utility.
 */
public final class EnvPropertiesConfig
        extends AbstractPropertiesConfig<EnvPropertiesConfigLoader> {
    private static final Object LOCK = new Object();

    private static EnvPropertiesConfig instance = null;

    private EnvPropertiesConfig() {
        super(true);
    }

    public static EnvPropertiesConfig getInstance() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new EnvPropertiesConfig();
                }
            }
        }
        return instance;
    }

    @Override
    protected EnvPropertiesConfigLoader newConfigProps() throws LoadConfigException {
        return new EnvPropertiesConfigLoader();
    }

    public String getZookeeperServerPrincipal() {
        return this.getConfig(BigdataUtilsGlobalConstants.ZOOKEEPER_SERVER_PRINCIPAL);
    }

    public String getLoginContextName() {
        return this.getConfig(BigdataUtilsGlobalConstants.LOGIN_CONTEXT_NAME);
    }

    public String getZookeeperServerPrincipalKey() {
        return this.getConfig(BigdataUtilsGlobalConstants.ZOOKEEPER_SERVER_PRINCIPAL_KEY);
    }

    public String getCoreSiteXml() {
        return this.getConfig(BigdataUtilsGlobalConstants.CORE_SITE_XML);
    }

    public String getHBaseSiteXml() {
        return this.getConfig(BigdataUtilsGlobalConstants.HBASE_SITE_XML);
    }

    public String getHDFSSiteXml() {
        return this.getConfig(BigdataUtilsGlobalConstants.HDFS_SITE_XML);
    }

    public String getJSecurityAuthLoginConfig() {
        return this.getConfig(BigdataUtilsGlobalConstants.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    }

    public String getJSecurityKrb5Conf() {
        return this.getConfig(BigdataUtilsGlobalConstants.JAVA_SECURITY_KRB5_CONF);
    }

    public String getRedisHostList() {
        return this.getConfig(BigdataUtilsGlobalConstants.REDIS_HOST_LIST);
    }

    public String getZookeeperPort() {
        return this.getConfig(BigdataUtilsGlobalConstants.ZOOKEEPER_PORT);
    }

    public String getZookeeperQuorum() {
        return this.getConfig(BigdataUtilsGlobalConstants.ZOOKEEPER_QUORUM);
    }

    public String getUserNameNormalMode() {
        return this.getConfig(BigdataUtilsGlobalConstants.USERNAME_NORMAL_MODE);
    }

    public String getHivePrincipal() {
        return this.getConfig(BigdataUtilsGlobalConstants.HIVE_PRINCIPAL);
    }

    public String getUserNameClientKrbKeyTabFile() {
        return this.getConfig(BigdataUtilsGlobalConstants.USERNAME_CLIENT_KERBEROS_KEYTAB_FILE);
    }

    public String getSafeMode() {
        return this.getConfig(BigdataUtilsGlobalConstants.SAFE_MODE);
    }

    public String getUserNameClientKrbPrincipal() {
        return this.getConfig(BigdataUtilsGlobalConstants.USERNAME_CLIENT_KERBEROS_PRINCIPAL);
    }

}

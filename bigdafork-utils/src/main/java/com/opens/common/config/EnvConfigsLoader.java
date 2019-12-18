package com.opens.common.config;

import com.opens.common.basic.BasicObservable;
import com.opens.common.constants.BigdataUtilsGlobalConstants;
import com.opens.common.exceptions.LoadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observer;

/**
 * This is a config load utility.
 */
public final class EnvConfigsLoader extends BasicObservable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvConfigsLoader.class);
    private static final Object LOCK = new Object();

    private static EnvConfigsLoader instance = null;

    private EnvConfigProperties configs = null;
    private boolean status = false;

    private EnvConfigsLoader() {
        this(null);
    }

    private EnvConfigsLoader(Observer observer) {
        super(observer);
        this.load();
    }

    public static EnvConfigsLoader getInstance() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new EnvConfigsLoader();
                }
            }
        }
        return instance;
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

    public String getUserNameClientKrbKeyTabFile() {
        return this.getConfig(BigdataUtilsGlobalConstants.USERNAME_CLIENT_KERBEROS_KEYTAB_FILE);
    }

    public String getSaveMode() {
        return this.getConfig(BigdataUtilsGlobalConstants.SAFE_MODE);
    }

    public String getUserNameClientKrbPrincipal() {
        return this.getConfig(BigdataUtilsGlobalConstants.USERNAME_CLIENT_KERBEROS_PRINCIPAL);
    }

    public String getConfig(String keyName) {
        if (status) {
            return this.configs.getProperty(keyName);
        } else {
            return null;
        }
    }

    private void load() {
        try {
            configs = new EnvConfigProperties();
            status = true;
            notify(status);
        } catch (LoadConfigException e) {
            LOGGER.error(e.getMessage());
            notify(status);
        }
    }

}

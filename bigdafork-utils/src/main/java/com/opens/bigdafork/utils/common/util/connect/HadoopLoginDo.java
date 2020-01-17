package com.opens.bigdafork.utils.common.util.connect;

import com.opens.bigdafork.common.base.AbstractBasicObservable;
import com.opens.bigdafork.common.base.IDo;
import com.opens.bigdafork.utils.common.config.EnvConfigsLoader;
import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import com.opens.bigdafork.utils.common.exceptions.InvalidLoginInfoException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observer;

/**
 * This is a common login module of hadoop platform using kerberos authentication.
 * There are three key points about login: principal, keytab file and krb5 configuration file that
 * we will use to invoke the final login method after it finishes generating JAAS information
 * and preparing zk principal settings in environment variables.
 * During this progress, it is necessary to check every needed field value out for successful authenticating.
 * If any details of login information is catched out wrong or invalid, the login would be failure so the system
 * should notify immediately to this observer to handle some relevant event,
 * such as shutdown the application right now.
 */
public class HadoopLoginDo extends AbstractBasicObservable implements IDo<Configuration, Configuration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopLoginDo.class);

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    public HadoopLoginDo() {
        this(null);
    }

    public HadoopLoginDo(Observer observer) {
        super(observer);
    }

    @Override
    public Configuration iDo(Configuration entries) {
        entries.setBoolean(BigdataUtilsGlobalConstants.SAFE_MODE, true);
        try {
            //jaas
            setJaasConf();
            setZKServerPrincipal();
            //login
            loginHadoop(entries);
        } catch (InvalidLoginInfoException e) {
            LOGGER.error(e.getMessage());
            this.notify(false);

        } catch (IOException e) {
            e.printStackTrace();
            this.notify(false);
        }

        return initializeOthers(entries);
    }

    public Configuration initializeOthers(Configuration entries) {
        return entries;
    }

    private synchronized void loginHadoop(Configuration configuration) throws IOException, InvalidLoginInfoException {
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        String principalName = env.getUserNameClientKrbPrincipal();
        String keyTabFilePath = env.getUserNameClientKrbKeyTabFile();
        String krb5ConfFilePath = env.getJSecurityKrb5Conf();
        LOGGER.debug("loginHadoop==");
        //some checks
        if (StringUtils.isBlank(principalName)) {
            throw new InvalidLoginInfoException("no principalName");
        }

        if (StringUtils.isBlank(keyTabFilePath)) {
            throw new InvalidLoginInfoException("no keyTabFilePath");
        }
        if (StringUtils.isBlank(krb5ConfFilePath)) {
            throw new InvalidLoginInfoException("no krb5ConfFilePath");
        }

        if (configuration == null) {
            throw new InvalidLoginInfoException("no hadoop configuration");
        }

        File keyTabFile = new File(keyTabFilePath);

        //check exist and isFile
        if (!keyTabFile.exists()) {
            throw new InvalidLoginInfoException("keyTabFile does not exist");
        }
        if (!keyTabFile.isFile()) {
            throw new InvalidLoginInfoException("keyTabFile is not a file on disk");
        }
        File krb5ConfFile = new File(krb5ConfFilePath);
        //check exist and is file
        if (!krb5ConfFile.exists()) {
            throw new InvalidLoginInfoException("krb5ConfFile does not exist");
        }
        if (!krb5ConfFile.isFile()) {
            throw new InvalidLoginInfoException("krb5ConfFile is not a file on disk");
        }

        setKrb5Conf(krb5ConfFile.getAbsolutePath());

        //set configuration for login
        UserGroupInformation.setConfiguration(configuration);

        try {
            LOGGER.debug("===== ： " + principalName);
            LOGGER.debug("===== ： " + keyTabFile.getAbsolutePath());
            UserGroupInformation.loginUserFromKeytab(principalName, keyTabFile.getAbsolutePath());
        } catch (IOException e) {
            //many reasone list
            LOGGER.error(String.format("login failed with %s and %s",
                    principalName, keyTabFilePath));
            LOGGER.warn(String.format("perhaps cause 1 is %s",
                    BigdataUtilsGlobalConstants.LOGIN_FAILED_CAUSE_PASSWORD_WRONG));
            LOGGER.warn(String.format("perhaps cause 2 is %s",
                    BigdataUtilsGlobalConstants.LOGIN_FAILED_CAUSE_TIME_WRONG));
            LOGGER.warn(String.format("perhaps cause 3 is %s",
                    BigdataUtilsGlobalConstants.LOGIN_FAILED_CAUSE_AES256_WRONG));
            LOGGER.warn(String.format("perhaps cause 4 is %s",
                    BigdataUtilsGlobalConstants.LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG));
            LOGGER.warn(String.format("perhaps cause 5 is %s",
                    BigdataUtilsGlobalConstants.LOGIN_FAILED_CAUSE_TIME_OUT));
            throw e;
        }

    }

    private void setKrb5Conf(String krb5ConfFile) {
        System.setProperty(BigdataUtilsGlobalConstants.JAVA_SECURITY_KRB5_CONF, krb5ConfFile);
        String krb5ConfPath = System.getProperty(BigdataUtilsGlobalConstants.JAVA_SECURITY_KRB5_CONF);
        //check null and equals
        if(krb5ConfPath == null) {
            throw new InvalidLoginInfoException("krb5 config file does not exist in system properties");
        }

        if (!krb5ConfPath.equals(krb5ConfFile)) {
            throw new InvalidLoginInfoException("krb5 config file is different with one in system properties");
        }

    }

    private void setZKServerPrincipal() throws InvalidLoginInfoException {
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        String zkServerPrincipalKey = env.getZookeeperServerPrincipalKey();
        String zkServerPrincipal = env.getZookeeperServerPrincipal();
        System.setProperty(zkServerPrincipalKey, zkServerPrincipal);
        String ret = System.getProperty(zkServerPrincipalKey);
        if(ret == null) {
            throw new InvalidLoginInfoException("zk server principal does not exist in system properties");
        }

        if (!ret.equals(zkServerPrincipal)) {
            throw new InvalidLoginInfoException("zk server principal is different with one in system properties");
        }
    }

    private void setJaasConf() throws InvalidLoginInfoException {
        EnvConfigsLoader env = EnvConfigsLoader.getInstance();
        String loginContextName = env.getLoginContextName();
        if (StringUtils.isBlank(loginContextName)) {
            throw new InvalidLoginInfoException("JAAS login contextName is invalid");
        }

        String principal = env.getUserNameClientKrbPrincipal();
        if (StringUtils.isBlank(principal)) {
            throw new InvalidLoginInfoException("JAAS login user principal name is invalid");
        }

        String keytabFile = env.getUserNameClientKrbKeyTabFile();
        if (StringUtils.isBlank(keytabFile)) {
            throw new InvalidLoginInfoException("JAAS keytabFile is invalid");
        }

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            throw new InvalidLoginInfoException(
                    String.format("CLIENT keytab file [%s] DOES NOT EXIST", userKeytabFile.getAbsolutePath()));
        }

        //set complete
        javax.security.auth.login.Configuration.setConfiguration(new JaasConfiguration(loginContextName,
                principal, userKeytabFile.getAbsolutePath()));

        //check set
        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            throw new InvalidLoginInfoException("there is no Jaas config file using by login config ");
        }

        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            throw new InvalidLoginInfoException("there is no any appConfiguration " +
                    "entries in Jaas config file using by login config ");
        }

        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (AppConfigurationEntry ent : entrys) {
            if (principal.equals(ent.getOptions().get("principal"))) {
                checkPrincipal = true;
            }
            if (IS_IBM_JDK) {
                LOGGER.debug("is_IBM_JDK");
                LOGGER.debug("jaas `s ukt : " + ent.getOptions().get("useKeyTab"));
                if (keytabFile.equals(ent.getOptions().get("useKeyTab"))) {
                    checkKeytab = true;
                }
            } else {
                LOGGER.debug("jaas `s ukt : " + ent.getOptions().get("keyTab"));
                if (keytabFile.equals(ent.getOptions().get("keyTab"))) {
                    checkKeytab = true;
                }
            }
            LOGGER.debug("configs` ukt : " + keytabFile);
        }

        if (!checkPrincipal) {
            throw new InvalidLoginInfoException("principal is different with one " +
                    "in Jaas config file using by login config ");
        }

        if (!checkKeytab) {
            throw new InvalidLoginInfoException("keytab file is different with one " +
                    "in Jaas config file using by login config ");
        }
    }

    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();

        private static final Map<String, String> KEYTAB_KRB_OPTIONS = new HashMap<>();

        static {
            String jaasEnvVar = System.getProperty(BigdataUtilsGlobalConstants.JAAS_DEBUG);
            if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }

            if (IS_IBM_JDK) {
                KEYTAB_KRB_OPTIONS.put("credsType", "both");
            } else {
                KEYTAB_KRB_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KRB_OPTIONS.put("useTicketCache", "false");
                KEYTAB_KRB_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KRB_OPTIONS.put("storeKey", "true");
            }

            KEYTAB_KRB_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        }

        private static final AppConfigurationEntry KEYTAB_KRB_LOGIN = new AppConfigurationEntry(
                KerberosUtil.getKrb5LoginModuleName(),
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                KEYTAB_KRB_OPTIONS);

        private static final AppConfigurationEntry[] KEYTAB_KRB_CONF = new AppConfigurationEntry[]{KEYTAB_KRB_LOGIN};

        private javax.security.auth.login.Configuration baseConfig;

        private String loginContextName;
        private boolean useTicketCache;
        private String keytabfile;
        private String principal;

        public JaasConfiguration(String loginContextName, String principal, String keytabfile) {
            this(loginContextName, principal, keytabfile, keytabfile == null || keytabfile.length() == 0);
        }

        public JaasConfiguration(String loginContextName, String principal,
                                 String keytabfile, boolean useTicketCache) {
            try {
                this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
            } catch (SecurityException e) {
                LOGGER.warn("can not get base config directly : " + e.getMessage());
                this.baseConfig = null;
            }
            this.keytabfile = keytabfile;
            this.loginContextName = loginContextName;
            this.principal = principal;
            this.useTicketCache = useTicketCache;

            initialKrbOptions();

            LOGGER.debug(String.format("JaasConfiguration loginContextName = %s,principal = %s," +
                    "keytabfile = %s,  useTicketCache = %s",
                    this.loginContextName, this.principal, this.keytabfile, this.useTicketCache));
        }

        private void initialKrbOptions() {
            if (!useTicketCache) {
                if (IS_IBM_JDK) {
                    KEYTAB_KRB_OPTIONS.put("useKeyTab", keytabfile);
                } else {
                    KEYTAB_KRB_OPTIONS.put("keyTab", keytabfile);
                    KEYTAB_KRB_OPTIONS.put("useKeyTab", "true");
                    KEYTAB_KRB_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
                }
            }
            KEYTAB_KRB_OPTIONS.put("principal", this.principal);
        }

        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (this.loginContextName.equals(appName)) {
                return KEYTAB_KRB_CONF;
            }

            if (this.baseConfig != null) {
                return baseConfig.getAppConfigurationEntry(appName);
            }

            return null;
        }
    }
}

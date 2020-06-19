package com.opens.bigdafork.utils.common.constants;

/**
 * This class maintains a collection of all the system constants.
 */
public final class BigdataUtilsGlobalConstants {

    private BigdataUtilsGlobalConstants() {}

    public static final String JVM_PROPERTIES_PATH = "properties.path";

    /**
     * Initialize the environment context with the specified parameters
     * from System Property identified by key #JVM_CONTEXT_PROPERTIES
     * If it is set by a class object outside of class#EnvConfigProperties object,
     * The class#EnvConfigProperties object will
     * bypass other ways the one initialize context to load the set of params.
     * Make sure the value this key pointers to is strictly in JSON string format.
     */
    public static final String JVM_CONTEXT_PROPERTIES = "context.properties";
    public static final String CLASSPATH_PROPERTIES_FILE_NAME = "env.properties";

    /**
     * To connect to a cluster in safe mode if it is set to value "true".
     */
    public static final String SAFE_MODE = "bigdafork.safe.mode";
    public static final String
            USERNAME_NORMAL_MODE = "bigdafork.username.normal.mode";

    // base
    public static final String
            ZOOKEEPER_SERVER_PRINCIPAL = "zookeeper.server.principal";

    public static final String
            ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal.key";

    public static final String HDFS_SITE_XML = "hdfs.site.xml";
    public static final String CORE_SITE_XML = "core.site.xml";
    public static final String HBASE_SITE_XML = "hbase.site.xml";
    public static final String YARN_SITE_XML = "yarn.site.xml";
    public static final String ZOOKEEPER_QUORUM = "zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "zookeeper.port";
    public static final String REDIS_HOST_LIST = "redis.host.list";
    public static final String
            USERNAME_CLIENT_KERBEROS_PRINCIPAL = "username.client.kerberos.principal";

    public static final String
            USERNAME_CLIENT_KERBEROS_KEYTAB_FILE = "username.client.kerberos.keytab.file";

    public static final
            String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    public static final String LOGIN_CONTEXT_NAME = "login.context.name";

    public static final String JAAS_DEBUG = "JAAS_DEBUG";

    public static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong pasword) keytab file and user does not match, " +
                    "you can kinit -k -t keytab user in client to check";
    public static final String LOGIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server does not match," +
                    "please check ntp out to remote server";
    public static final String LOGIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 is not supported by default jdk/jre, " +
                    "we need copy local_policy.jar and US_export_policy.jar " +
                    "from remote server in path /opt/.../jdk/jre/security/";
    public static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(oreno rule) principal format is not supported by default, \" +\n" +
                    "                    \"we need add property hadoop.security.auth_to_local \" +\n" +
                    "                    \"in c-site xml value RULE:[1:$1] RULE:[2:$1]";
    public static final String LOGIN_FAILED_CAUSE_TIME_OUT =
            "(time out) cannot connect to kdc server or there is a " +
                    "fire wall in the network";

    public static final String HIEV_JDBC_URL_KEY = "hiveJdbcUrl";
    public static final String HIVE_PRINCIPAL = "hive.client.kerberos.principal";

    public static final String IS_SAVE_MODE = "true";
}

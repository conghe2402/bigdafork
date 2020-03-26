package com.opens.bigdafork.datatask.config;

/**
 * Constants.
 */
public final class DataTaskConstants {
    public static final String KEY_CONFIG_C1_PATH = "config.c1.path";
    public static final String KEY_CONFIG_C2_PATH = "config.c2.path";
    public static final String KEY_RECORD_DIR = "record.dir";
    public static final String KEY_NAS_CONFIG_PATH = "nas.config.path";
    public static final String KEY_ENV_CONFIG_PATH = "env.config.path";
    public static final String USERNAME_CLIENT_KEYTAB_FILE = "username.client.keytab.file";
    public static final String USERNAME_CLIENT_KERBEROS_PRINCIPAL = "username.client.kerberos.principal";

    public static final String ENV_CONFIG = "env.properties";


    //params of c1
    public static final String KEY_ENGINE_DEFUALT_SPARKSQL = "engine.default.sparksql";
    public static final String KEY_ENGINE_DEFUALT_HIVEONSPARK = "engine.default.hiveonspark";
    public static final String KEY_ENGINE_DEFUALT_HIVE = "engine.default.hive";
    public static final String KEY_ENGINE_DEFUALT_PRIORITY = "engine.default.priority";
    public static final String KEY_SQL_DDL_ENGINE = "sql.ddl.engine";


    public static final int HIVE = 1;
    public static final int HIVE_ON_SPARK = 2;
    public static final int SPARK_SQL = 3;
    public static final String DEFAULT_ENGINE_PRIOS = "1,2,3";

    public static final int C2_RES_LEVEL_DEFAULT = 0;
    public static final int C2_RES_LEVEL_1 = 1;
    public static final int C2_RES_LEVEL_2 = 2;
    public static final int C2_RES_LEVEL_3 = 3;
    public static final int C2_RES_LEVEL_4 = 4;

    public static final String HIVE_ON_MR_NAME = "hiveonmr";
    public static final String HIVE_ON_SPARK_NAME = "hiveonspark";
    public static final String SPARK_SQL_NAME = "sparkSQL";

    public static final String SQL_INDEX_PREFIX = "sql";

    public static final String[] ENGINE_NAMES = {"", HIVE_ON_MR_NAME,
        HIVE_ON_SPARK_NAME, SPARK_SQL_NAME};

    public static final String RECORD_SEPERATOR = "\u0905";

    // C2 custom yaml CONFIG def
    /*
    tasks:
        params:
            a1: 3
            map: 3
        sql1:
            engine:
                normal: 1
                timeout: 120
                standby: 2
            params:
                mapreduec.map.memory.mb: 4096
                mapreduce.reduce.java.opts: "+XX: DDD  –Xms40m"
                mapreduce.reduce.java1.opts: "+XX: DDD  –Xms40m"
     */
    public static final String FIELD_TASK = "tasks";
    public static final String FIELD_PARAMS = "params";
    public static final String FIELD_SQL_ENGINE = "engine";
    public static final String FIELD_SQL_ENGINE_NORMAL = "normal";
    public static final String FIELD_SQL_ENGINE_STANDBY = "standby";
    public static final String FIELD_SQL_ENGINE_TIMEOUT = "timeout";

    public static final String SPARKSQL_RESULT_SUCC = "ebdaComp123";
    public static final String SPARKSQL_RESULT_FAIL = "ebdaFail123";

    private DataTaskConstants() {}
}

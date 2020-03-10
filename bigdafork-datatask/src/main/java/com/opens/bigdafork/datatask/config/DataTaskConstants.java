package com.opens.bigdafork.datatask.config;

/**
 * Constants.
 */
public final class DataTaskConstants {
    public static final String KEY_CONFIG_C1_PATH = "config.c1.path";
    public static final String KEY_CONFIG_C2_PATH = "config.c2.path";
    public static final String KEY_RECORD_DIR = "record.dir";
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

    private DataTaskConstants() {}
}

package com.opens.bigdafork.utils.common.util.connect;

import com.google.common.collect.Lists;
import com.opens.bigdafork.common.base.IDo;
import com.opens.bigdafork.utils.common.config.EnvPropertiesConfig;
import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * This is a master knows how to make and provide
 * a set of strategies that make a connection to servers.
 */
public final class ConnectMaster {

    public static final String REQ_HBASE_CONN = "Hbase";
    public static final String REQ_REDIS_CONN = "Redis";
    public static final String REQ_HIVE_CONN = "hive";
    public static final String REQ_YARN_CONN = "yarn";

    private ConnectMaster() {}

    public static List<IDo<Configuration, Configuration>> getConnStrategy(String req) {
        List<IDo<Configuration, Configuration>> connDoList = Lists.newArrayList();
        String safeMode = EnvPropertiesConfig.getInstance().getSafeMode();
        boolean isSafeMode = BigdataUtilsGlobalConstants.IS_SAVE_MODE
                .equalsIgnoreCase(safeMode);

        if (REQ_HBASE_CONN.equals(req)) {
            //1 configs
            connDoList.add(new HBaseConfDo());
        } else if (REQ_HIVE_CONN.equals(req)) {
            connDoList.add(new HiveConfDo());
            connDoList.add(new HiveUrlDo(isSafeMode));
        } else if (REQ_YARN_CONN.equals(req)) {
            connDoList.add(new YarnConf());
            connDoList.add(new HadoopLoginDo());
        }

        if (isSafeMode && !REQ_YARN_CONN.equals(req)) {
            //2 login
            connDoList.add(new HadoopLoginDo());
        }
        return connDoList;
    }
}

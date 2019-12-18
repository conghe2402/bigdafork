package com.opens.common.util.connect;

import com.google.common.collect.Lists;
import com.opens.common.basic.IDo;
import com.opens.common.config.EnvConfigsLoader;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * This is a master knows how to make and provide
 * a set of strategies that make a connection to servers.
 */
public final class ConnectMaster {

    public static final String REQ_HBASE_CONN = "Hbase";
    public static final String REQ_REDIS_CONN = "Redis";
    public static final String IS_SAVE_MODE = "true";

    private ConnectMaster() {}

    public static List<IDo<Configuration, Configuration>> getConnStrategy(String req) {
        List<IDo<Configuration, Configuration>> connDoList = Lists.newArrayList();
        String saveMode = EnvConfigsLoader.getInstance().getSaveMode();
        if (REQ_HBASE_CONN.equals(req)) {
            //1 configs
            connDoList.add(new HBaseConfDo());

            if (IS_SAVE_MODE.equalsIgnoreCase(saveMode)) {
                //2 login
                connDoList.add(new HBaseLoginDo());
            }
        }
        return connDoList;
    }
}

package com.opens.bigdafork.utils.common.util;

import com.opens.bigdafork.utils.common.basic.IDo;
import com.opens.bigdafork.utils.common.util.connect.ConnectMaster;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * This is responsible for executing
 * all of activities of preparation
 * for initialising client context of hadoop components.
 */
public final class DefaultConnect4Ready {
    private DefaultConnect4Ready() {

    }

    public static Configuration ready4Hbase(Configuration configuration) {
        return ready(ConnectMaster.REQ_HBASE_CONN, configuration);
    }

    public static Configuration ready4Hive(Configuration configuration) {
        return ready(ConnectMaster.REQ_HIVE_CONN, configuration);
    }

    private static Configuration ready(String componentType, Configuration configuration) {
        List<IDo<Configuration, Configuration>> connDoList =
                ConnectMaster.getConnStrategy(componentType);

        for (IDo<Configuration, Configuration> doItem : connDoList) {
            configuration = doItem.iDo(configuration);
        }
        return configuration;
    }
}

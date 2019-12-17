package com.opens.common.util;

import com.opens.common.basic.IDo;
import com.opens.common.util.connect.ConnectMaster;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * This is responsible for executing
 * all of activities of preparation
 * for initialising hbase client context.
 */
public class HBaseConnect4Ready {
    //private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnect4Ready.class);

    private Configuration configuration;

    private List<IDo<Configuration, Configuration>> connDoList = null;

    public HBaseConnect4Ready(Configuration configuration) {
        this.configuration = configuration;
    }

    public void ready() {
        connDoList = ConnectMaster.getConnStrategy(ConnectMaster.REQ_HBASE_CONN);
        for (IDo<Configuration, Configuration> doit : connDoList) {
            this.configuration = doit.iDo(configuration);
        }
    }
}

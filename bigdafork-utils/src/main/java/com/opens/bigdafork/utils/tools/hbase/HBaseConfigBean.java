package com.opens.bigdafork.utils.tools.hbase;

import lombok.Data;

import java.io.Serializable;

/**
 * Parameters for control to initialise the hbase client`s operations.
 */
@Data
public class HBaseConfigBean implements Serializable {
    private String tableName;

    private int cacheNum;

    private int batchNum;

    private boolean cacheBlock = false;

    private int clientConnThreadNum;

    private int balanceRatioNum = 0;

    private int leasePeriodNum;

    private boolean useNewConfig = false;
}

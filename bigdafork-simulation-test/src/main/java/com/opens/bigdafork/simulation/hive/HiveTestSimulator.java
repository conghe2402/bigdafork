package com.opens.bigdafork.simulation.hive;

import com.opens.bigdafork.utils.tools.hive.manage.HiveManageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * It is a hive test simulator that generates automatically dummy data what it inserts into
 * the hive tables for works on your testing, tuning, optimization
 * and stuffs.
 */
public final class HiveTestSimulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTestSimulator.class);

    private HiveTestSimulator() {}

    public static void main(String[] args) {
        LOGGER.info("start......");
        HiveManageUtils hiveManageUtils = new HiveManageUtils();

        String tableName = "M_TBL_CUST_STAT_WEEK";
        Map<String, HiveManageUtils.HiveField> fm =  hiveManageUtils.getHiveFieldsOfHiveTable(tableName);

        for (Map.Entry<String, HiveManageUtils.HiveField> e : fm.entrySet()) {
            LOGGER.info(e.getKey() + " : " + e.getValue().getFieldType());
        }
        LOGGER.info("complete......");
    }
}

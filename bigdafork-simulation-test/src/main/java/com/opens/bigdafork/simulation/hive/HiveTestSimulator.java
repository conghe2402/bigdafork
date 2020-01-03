package com.opens.bigdafork.simulation.hive;

import com.opens.bigdafork.simulation.hive.build.Initializer;
import com.opens.bigdafork.simulation.hive.build.TableDataBuilder;
import com.opens.bigdafork.utils.tools.hive.manage.HiveManageUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * It is a hive test simulator that generates automatically dummy data what it inserts into
 * the hive tables for works on your testing, tuning, optimization and stuffs.
 */
public final class HiveTestSimulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTestSimulator.class);

    private HiveTestSimulator() {}

    public static void main(String[] args) {
        LOGGER.info("start......");
        HiveManageUtils hiveManageUtils = new HiveManageUtils();

        Configuration env = hiveManageUtils.getEnvConfiguration();
        int rowNumber = 10;

        String[] tableNames = {"M_TBL_CUST_STAT_WEEK"};
        if (args != null && args.length > 1) {
            tableNames = args[0].split(",");
        }
        Set<String> tableSet = new HashSet();
        Collections.addAll(tableSet, tableNames);

        Initializer initializer = new Initializer(env, tableSet);
        initializer.doInitial();

        for (Iterator<String> it = tableSet.iterator(); it.hasNext();) {
            String tableName = it.next();
            LOGGER.info("begin to simulate " + tableName);
            Map<String, HiveManageUtils.HiveField> fm = hiveManageUtils.getHiveFieldsOfHiveTable(tableName);

            /*
            for (Map.Entry<String, HiveManageUtils.HiveField> e : fm.entrySet()) {
                LOGGER.info(e.getKey() + " : " + e.getValue().getFieldType());
            }
            */

            TableDataBuilder tableDataBuilder = new TableDataBuilder(env, tableName, fm, rowNumber);
            tableDataBuilder.outputDataFile();

            tableDataBuilder.loadLocalData();
            LOGGER.info("done with simulating " + tableName);
        }

        LOGGER.info("complete......");
    }
}

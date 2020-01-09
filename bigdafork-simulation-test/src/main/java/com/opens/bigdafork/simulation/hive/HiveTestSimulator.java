package com.opens.bigdafork.simulation.hive;

import com.opens.bigdafork.simulation.common.Constants;
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
        long rowNumber = 0;
        String[] tableNames = {"TBL_LOANTRANLOGINFO_TMP1", ""};

        int i = 0;
        LOGGER.info("params : ");
        if (args != null && args.length > i) {
            tableNames = args[i++].toUpperCase().split(",");
        }
        for (String table : tableNames) {
            LOGGER.info(table);
        }

        if (args != null && args.length > i) {
            rowNumber = Integer.parseInt(args[i++]);
        }
        LOGGER.info("rowNumber of each table : " + rowNumber);

        Set<String> tableSet = new HashSet();
        Collections.addAll(tableSet, tableNames);

        HiveManageUtils hiveManageUtils = new HiveManageUtils();
        Configuration env = hiveManageUtils.getEnvConfiguration();

        Initializer initializer = new Initializer(env, tableSet);
        Set<Initializer.TableConf> tableConfSet = initializer.doInitial();

        if (tableConfSet == null || tableConfSet.size() == 0) {
            LOGGER.info("no table need initialize...");
            return;
        }

        for (Iterator<Initializer.TableConf> it = tableConfSet.iterator(); it.hasNext();) {
            Initializer.TableConf tableConf = it.next();
            String mTableName = String.format("%s%s", Constants.SIMULATE_PREFIX, tableConf.getTableName());
            LOGGER.info("begin to simulate " + mTableName);
            if (rowNumber <= 0) {
                rowNumber = tableConf.getRowNumber();
            }
            Map<String, HiveManageUtils.HiveField> fm = hiveManageUtils.getHiveFieldsOfHiveTable(mTableName);

            /*
            for (Map.Entry<String, HiveManageUtils.HiveField> e : fm.entrySet()) {
                LOGGER.info(e.getKey() + " : " + e.getValue().getFieldType() + " : " + e.getValue().isMK());
            }
            */

            TableDataBuilder tableDataBuilder = new TableDataBuilder(env, mTableName, fm, rowNumber);
            tableDataBuilder.outputDataFile();

            if (!tableDataBuilder.putIntoHDFS()) {
                LOGGER.info("put file into hfds fail");
                return;
            }

            tableDataBuilder.loadHDFSData2TmpTable();

            tableDataBuilder.insert2MTable();

            LOGGER.info("done with simulating " + mTableName);
        }

        LOGGER.info("complete......");
    }
}

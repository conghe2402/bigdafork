package com.opens.bigdafork.simulation.hive.build;

import com.google.common.collect.Lists;
import com.opens.bigdafork.simulation.common.Constants;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Initializer.
 */
public class Initializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Initializer.class);
    private static final String CONF_FILE = "/root/workspace/bigdafork-simulation-test-1.0-SNAPSHOT/hive-tables.ini";
    private List<String> tableNeedList = Lists.newArrayList();
    private Set<String> tables;
    private Configuration env;

    public Initializer(Configuration env, Set<String> tables) {
        this.env = env;
        this.tables = new HashSet();
        tableNeedList.addAll(tables);
    }

    /**
     * To initialize all of tables needed to do.
     * @return
     */
    public Set<String> doInitial() {
        LOGGER.info("initializing...");
        File confFile = new File(CONF_FILE);
        if (!confFile.exists()) {
            LOGGER.warn("no conf file");
            System.exit(0);
        }

        try(BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(confFile)))) {
            String cmd = br.readLine();
            while (cmd != null && !cmd.trim().equals("")) {
                LOGGER.debug(cmd);
                initialTable(cmd);
                cmd = br.readLine();
            }
            return tables;
        } catch (IOException | SQLException
                | ClassNotFoundException | IllegalAccessException
                | InstantiationException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private void initialTable(String tableCmd) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        String[] cmds = tableCmd.split(",");
        String tableName = cmds[0];
        if (!isNeedInitial(tableName)) {
            LOGGER.debug("ignore table : " + tableName);
            return;
        }
        tables.add(tableName);

        String mkColName = cmds[1];
        String mkComments = cmds[2];
        String parColName = cmds[3];
        String partComment = cmds[4];
        try (Connection connection = HiveOpUtils.getConnection(this.env)) {
            HiveOpUtils.execDDL(connection, getDropTableSQL(tableName));
            HiveOpUtils.execDDL(connection, getCreateTableSQL(tableName));
            HiveOpUtils.execDDL(connection, getCommentSQL(tableName,
                    mkColName, mkComments));
            // partition field can not be add comments in this way except through meta database
            //HiveOpUtils.execDDL(connection, getCommentSQL(tableName,
            //        parColName, partComment));
        }
    }

    private String getDropTableSQL(String tableName) {
        String sql = String.format("drop table if exists %s%s",
                Constants.SIMULATE_PREFIX, tableName);
        LOGGER.debug(sql);
        return sql;
    }

    private String getCreateTableSQL(String tableName) {
        String sql = String.format("create table if not exists %s%s like %s",
                Constants.SIMULATE_PREFIX, tableName, tableName);
        LOGGER.debug(sql);
        return sql;
    }

    /**
     * can not change type.
     * @param tableName
     * @param colName
     * @param comments
     * @return
     */
    private String getCommentSQL(String tableName, String colName,
                                   String comments) {
        String sql = String.format("alter table %s%s change column %s %s string comment '%s'",
                Constants.SIMULATE_PREFIX, tableName, colName, colName, comments);
        LOGGER.debug(sql);
        return sql;
    }

    private boolean isNeedInitial(String tableName) {
        return tableNeedList.contains(tableName);
    }
}

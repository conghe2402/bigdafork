package com.opens.bigdafork.simulation.hive.build;

import com.google.common.collect.Lists;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Initializer.
 */
public class Initializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Initializer.class);
    private static final String SIMULATE_PREFIX = "M_";
    private static final String CONF_FILE = "hive-tables.ini";
    private List<String> tableNeedList = Lists.newArrayList();
    private Connection connection = null;

    public Initializer(Set<String> tables) {
        tableNeedList.addAll(tables);
    }

    /**
     * To initialize all of tables needed to do.
     */
    public void doInitial() {
        LOGGER.info("initializing...");
        File confFile = new File(CONF_FILE);
        if (!confFile.exists()) {
            LOGGER.warn("no conf file");
        }

        connection = HiveOpUtils.getConnection();

        try(BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(confFile)))) {
            String cmd = null;
            while ((cmd = br.readLine()) != null && !cmd.trim().equals("")) {
                LOGGER.debug(cmd);
                initialTable(cmd);
            }

        } catch (IOException | SQLException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        } finally {
            HiveOpUtils.closeConnection(this.connection);
        }
    }

    private void initialTable(String tableCmd) throws SQLException {
        String[] cmds = tableCmd.split(",");
        String tableName = cmds[0];
        if (!isNeedInitial(tableName)) {
            return;
        }
        String mkColName = cmds[1];
        String mkComments = cmds[2];
        String parColName = cmds[3];
        String partComment = cmds[4];
        HiveOpUtils.execDDL(this.connection, getDropTableSQL(tableName));
        HiveOpUtils.execDDL(this.connection, getCreateTableSQL(tableName));
        HiveOpUtils.execDDL(this.connection, getCommentSQL(tableName,
                mkColName, mkComments));
        HiveOpUtils.execDDL(this.connection, getCommentSQL(tableName,
                parColName, partComment));
    }

    private String getDropTableSQL(String tableName) {
        return String.format("drop table if exists %s%s",
                SIMULATE_PREFIX, tableName);
    }

    private String getCreateTableSQL(String tableName) {
        return String.format("create table if not exists %s%s like %s",
                SIMULATE_PREFIX, tableName, tableName);
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
        return String.format("alter table %s%s change column %s %s comment '%s'",
                SIMULATE_PREFIX, tableName, colName, colName, comments);
    }

    private boolean isNeedInitial(String tableName) {
        return tableNeedList.contains(tableName);
    }
}

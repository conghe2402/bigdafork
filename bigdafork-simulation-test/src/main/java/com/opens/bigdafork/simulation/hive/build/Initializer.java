package com.opens.bigdafork.simulation.hive.build;

import com.google.common.collect.Lists;
import com.opens.bigdafork.simulation.common.Constants;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import jodd.util.StringUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Initializer.
 */
public class Initializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Initializer.class);
    private static final String CONF_FILE = System.getProperty("user.dir") + "/hive-tables.ini";
    //private static final String CONF_FILE =
    // "D:\\github\\bigdafork\\bigdafork-simulation-test\\src\\main\\resources/hive-tables.ini";
    private List<String> tableNeedList = Lists.newArrayList();
    private Set<String> tables = new HashSet();
    private Set<TableConf> tableConfs = new HashSet();
    private Configuration env;

    public Initializer(Configuration env, Set<String> tables) {
        this.env = env;
        tableNeedList.addAll(tables);
    }

    /**
     * To initialize all of tables needed to do.
     * @return
     */
    public Set<TableConf> doInitial() {
        LOGGER.info("initializing...");
        LOGGER.debug("try to load : " + CONF_FILE);
        File confFile = new File(CONF_FILE);
        if (!confFile.exists()) {
            LOGGER.warn("no conf file");
            System.exit(0);
        }

        try(BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(confFile)))) {
            String cmd = br.readLine();
            while (cmd != null) {
                if (!cmd.trim().startsWith("#")
                        && !cmd.trim().equals("")) {
                    initialTable(cmd);
                }

                cmd = br.readLine();
            }
            return tableConfs;
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
        Map<String, String> fieldTypeMap = new HashMap<>();
        String[] cmds = tableCmd.split(",", -1);
        int index = 0;
        String tableName = cmds[index++];
        LOGGER.info("try to initial : " + tableName);
        if (!isNeedInitial(tableName)) {
            LOGGER.info("ignore table : " + tableName);
            return;
        }

        if (cmds.length < 5) {
            LOGGER.info("config number less than 5 , ignore table : " + tableName);
            return;
        }

        long rn = Long.parseLong(cmds[index++]);
        TableConf tableConf = new TableConf(tableName, rn);
        tableConfs.add(tableConf);
        tables.add(tableName);

        String mkColName = cmds[index++];
        String mkComments = cmds[index++];

        String tableStoreFormat = cmds[index++];

        //fieldName , item value
        LinkedHashMap<String, String> fom = new LinkedHashMap();

        if (cmds.length > 6) {
            for (int i = index + 1; i < cmds.length; i+=2) {
                fom.put(cmds[i - 1], cmds[i]);
            }
        }

        //to test
        /*
        for (Map.Entry<String, String> item : fom.entrySet()) {
            FieldValueGen f = (FieldValueGen)ConfigParser.parseItem(tableName, item.getKey(), item.getValue());
            if (f != null) {
                for (int k=0;k<100;k++) {
                    System.out.println(f.getValue());
                }
            }
        }
        */

        try (Connection connection = HiveOpUtils.getConnection(this.env)) {
            HiveOpUtils.dropTable(connection, String.format("%s%s",
                    Constants.SIMULATE_PREFIX, tableName));
            HiveOpUtils.execDDL(connection, getCreateTableSQL(connection, tableName,
                    tableStoreFormat, fieldTypeMap));
            HiveOpUtils.execDDL(connection, getCommentSQL(tableName,
                    mkColName, fieldTypeMap.get(mkColName), mkComments));
            //create txt temp table
            HiveOpUtils.dropTable(connection, String.format("%s%s%s",
                    Constants.SIMULATE_PREFIX, tableName, Constants.TEMP_TABLE_SUFFIX));
            HiveOpUtils.execDDL(connection, getTxtTableCreateSQL(connection, tableName));
            // partition field can not be add comments in this way except through meta database
            //HiveOpUtils.execDDL(connection, getCommentSQL(tableName,
            //        parColName, partComment));

            for (Map.Entry<String, String> item : fom.entrySet()) {
                HiveOpUtils.execDDL(connection, getCommentSQL(tableName,
                        item.getKey(), fieldTypeMap.get(item.getKey()), item.getValue()));
            }
        }
        LOGGER.info("done with initial : " + tableName);
    }

    /**
     * generate Txt table sql.
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    private String getTxtTableCreateSQL(Connection connection, String tableName) throws SQLException {
        String cmd = String.format("show create table %s%s",
                Constants.SIMULATE_PREFIX, tableName);
        LOGGER.debug(cmd);
        ResultSet rs = HiveOpUtils.execQuery(connection, cmd);

        StringBuilder createSQL = new StringBuilder();
        while (rs.next()) {
            String parts = rs.getString(1);
            //LOGGER.info(parts);
            if (parts.trim().startsWith("CREATE TABLE `")) {
                parts = String.format("CREATE TABLE `%s%s%s` (",
                        Constants.SIMULATE_PREFIX, tableName, Constants.TEMP_TABLE_SUFFIX);
            }
            if (parts.trim().startsWith("ROW FORMAT ")) {
                break;
            }
            createSQL.append(parts).append(" ");
        }
        createSQL.append(" row format delimited fields terminated by ',' lines terminated by '\\n' stored as textfile");
        LOGGER.debug(createSQL.toString());
        return createSQL.toString();
    }

    private String getDropTableSQL(String tableName) {
        String sql = String.format("drop table if exists %s%s",
                Constants.SIMULATE_PREFIX, tableName);
        LOGGER.debug(sql);
        return sql;
    }

    private String getCreateTableSQL(Connection connect, String tableName, String storeFormat,
                                     Map<String, String> fieldTypeMap) {
        //String sql = String.format("create table if not exists %s%s like %s",
        //        Constants.SIMULATE_PREFIX, tableName, tableName);
        if (StringUtils.isBlank(storeFormat)) {
            storeFormat = "textfile";
        }
        String sql = makeCreateTableSQL(connect, tableName, storeFormat, fieldTypeMap);
        LOGGER.debug(sql);
        return sql;
    }

    private String makeCreateTableSQL(Connection connect, String tableName, String storeFormat,
                                      Map<String, String> fieldTypeMap) {
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("create table ").append(String.format("%s%s",
                Constants.SIMULATE_PREFIX, tableName)).append(" (");

        try(ResultSet rs = connect.createStatement()
                .executeQuery(String.format("desc formatted %s", tableName))) {
            boolean hasCol = false;
            int colNum = 0;
            boolean hasPart = false;
            int lineNum = 0;
            while(rs.next()) {
                if (!hasPart && rs.getString(1).startsWith("# col_name")) {
                    hasCol = true;
                }
                if (hasCol) {
                    colNum++;
                }
                if (colNum >= 3 && !rs.getString(1).trim().equals("")) {
                    fieldTypeMap.put(rs.getString(1), rs.getString(2));
                    createTableSQL.append(rs.getString(1)).append(" ")
                            .append(rs.getString(2)).append(",");
                }
                if (colNum >=3 && hasCol && rs.getString(1).trim().equals("")) {
                    createTableSQL.deleteCharAt(createTableSQL.lastIndexOf(","));
                    createTableSQL.append(")");
                    colNum = 0;
                    hasCol = false;
                }

                if (rs.getString(1).startsWith("# Partition")) {
                    hasPart = true;
                    createTableSQL.append(" partitioned by (");
                }

                if (hasPart) {
                    lineNum++;
                }
                if (lineNum >= 4 && hasPart && !rs.getString(1).trim().equals("")) {
                    createTableSQL.append(rs.getString(1))
                            .append(" ").append(rs.getString(2))
                            .append(",");
                }
                if (lineNum >= 4 && hasPart && rs.getString(1).trim().equals("")) {
                    createTableSQL.deleteCharAt(createTableSQL.lastIndexOf(","));
                    createTableSQL.append(")");
                    break;
                }
            }
            createTableSQL.append(" stored as ").append(storeFormat);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }

        return createTableSQL.toString();
    }

    /**
     * can not change type.
     * @param tableName
     * @param colName
     * @param comments
     * @return
     */
    private String getCommentSQL(String tableName, String colName, String typeName,
                                   String comments) {
        String sql;
        if (StringUtil.isBlank(typeName)) {
            sql = String.format("alter table %s%s change column %s %s string comment '%s'",
                    Constants.SIMULATE_PREFIX, tableName, colName, colName, comments);
        } else {
            sql = String.format("alter table %s%s change column %s %s %s comment '%s'",
                    Constants.SIMULATE_PREFIX, tableName, colName, colName, typeName, comments);
        }

        LOGGER.debug(sql);
        return sql;
    }

    private boolean isNeedInitial(String tableName) {
        return tableNeedList.contains("ALL") || tableNeedList.contains(tableName);
    }

    /**
     * TableConf.
     */
    public class TableConf {
        @Getter @Setter
        private String tableName;
        @Getter @Setter
        private long rowNumber;

        public TableConf(String tableName,
                         long rowNumber) {
            this.rowNumber = rowNumber;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            return this.tableName.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            TableConf other = (TableConf)obj;
            return this.tableName.equals(other.getTableName());
        }
    }
}

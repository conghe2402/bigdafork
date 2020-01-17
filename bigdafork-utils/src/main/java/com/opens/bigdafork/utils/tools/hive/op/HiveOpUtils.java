package com.opens.bigdafork.utils.tools.hive.op;

import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Hive op utils.
 */
public final class HiveOpUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveOpUtils.class);

    private HiveOpUtils() {}

    public static Connection getConnection(Configuration env) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        if (env.getBoolean(BigdataUtilsGlobalConstants.SAFE_MODE, false)) {
            //safe mode
            return getConnectionSafeMode(env.get(BigdataUtilsGlobalConstants.HIEV_JDBC_URL_KEY));
        } else {
            return getConnectionNoSafeMode();
        }
    }

    /**
     * To get a connection to  hive server in safe mode.
     * @param url
     * @return
     */
    public static Connection getConnectionSafeMode(String url) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        Connection connection;
        try {
            LOGGER.debug("conn url : " + url);
            Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
            connection = DriverManager.getConnection(url, "", "");
        } catch (InstantiationException | IllegalAccessException
                | ClassNotFoundException | SQLException e) {
            LOGGER.error("when get a connection, throws an exception : " + e.getMessage());
            throw (e);
        }
        return connection;
    }

    public static Connection getConnectionNoSafeMode() {
        return null;
    }

    /**
     * To exec dll statement with connection obj.
     * @param connection
     * @param ddl
     */
    public static void execDDL(Connection connection, String ddl) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            statement.execute();
        }
    }

    public static void execDDL(Configuration env, String sql) {
        LOGGER.debug(sql);
        try (Connection connection = getConnection(env)) {
            HiveOpUtils.execDDL(connection, sql);
        } catch (SQLException | IllegalAccessException
                | InstantiationException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static ResultSet execQuery(Connection connection, String ddl) throws SQLException {
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(ddl);
    }

    /**
     * drop a hive table.
     */
    public static void dropTable(Connection connection, String tableName) throws SQLException {
        String sql = String.format("drop table if exists %s", tableName);
        LOGGER.debug(sql);
        HiveOpUtils.execDDL(connection, sql);
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}

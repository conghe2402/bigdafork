package com.opens.bigdafork.utils.tools.hive.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Hive op utils.
 */
public final class HiveOpUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveOpUtils.class);

    private HiveOpUtils() {}

    /**
     * To get a connection to  hive server.
     * @return
     */
    public static Connection getConnection() {
        return null;
    }

    /**
     * To exec dll statement.
     * @param connection
     * @param ddl
     */
    public static void execDDL(Connection connection, String ddl) {
        return;
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

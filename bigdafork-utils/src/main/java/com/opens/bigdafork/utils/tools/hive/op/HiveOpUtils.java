package com.opens.bigdafork.utils.tools.hive.op;

import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
     * To exec dll statement.
     * @param connection
     * @param ddl
     */
    public static void execDDL(Connection connection, String ddl) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            statement.execute();
        }
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

package com.opens.bigdafork.utils.tools.hive.manage;

import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import com.opens.bigdafork.utils.common.exceptions.LoadConfigException;
import com.opens.bigdafork.utils.common.util.DefaultConnect4Ready;
import com.opens.bigdafork.utils.tools.AbstractManageUtils;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
* To provide features as below:
* 1. If we specify some table names of which tables have existed, the procedure
* will parse the structure of these tables.
*/
public class HiveManageUtils extends AbstractManageUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveManageUtils.class);

    private String connUrl = null;

    public HiveManageUtils() {
        super();
    }

    public HiveManageUtils(Properties properties) {
        super(properties);
    }

    /**
     * To get a fields list using the given table name.
     * @param hiveTableName
     * @return
     */
    public Map<String, HiveField> getHiveFieldsOfHiveTable(String hiveTableName) {
        LOGGER.debug("getHiveFieldsOfHiveTable...");
        Map<String, HiveField> fieldsMap = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("describe %s", hiveTableName))) {

            ResultSetMetaData rsmd = rs.getMetaData();
            LOGGER.debug(rsmd.getColumnCount() + "");
            int i = 0;
            while (rs.next()) {
                LOGGER.debug("getTableName : " + rsmd.getTableName(i));
                LOGGER.debug("getCatalogName : " + rsmd.getCatalogName(i));
                LOGGER.debug("getColumnLabel : " + rsmd.getColumnLabel(i));
                LOGGER.debug("getColumnName : " + rsmd.getColumnName(i));
                LOGGER.debug("getScale : " + rsmd.getScale(i) + "");
                LOGGER.debug("getPrecision : " + rsmd.getPrecision(i) + "");
                LOGGER.debug("getCatalogName : " + rsmd.getCatalogName(i) + "");
                LOGGER.debug("getColumnType : " + rsmd.getColumnType(i) + "");
                LOGGER.debug("getColumnTypeName : " + rsmd.getColumnTypeName(i) + "");
                LOGGER.debug("getSchemaName : " + rsmd.getSchemaName(i) + "");
                HiveField field = new HiveField();
                field.setFieldName(rs.getString(1));
                field.setFieldType(rs.getString(2));
                field.setIndex(i++);
                LOGGER.debug(rs.getMetaData().toString());
                fieldsMap.put(field.getFieldName(), field);
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("getHiveFieldsOfHiveTable, an exception occurs ： " + e.getMessage());
        }
        return fieldsMap;
    }

    @Override
    protected void initialize() {
        this.setConfiguration(new Configuration());
        DefaultConnect4Ready.ready4Hive(this.getConfiguration());
        LOGGER.debug("hive conn url : " + this.getHiveConnUrl());
    }

    private Connection getConnection() throws Exception {
        String url = getHiveConnUrl();
        if(StringUtils.isBlank(url)) {
            throw new LoadConfigException("can not get hive url!");
        }

        Connection connection = null;
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

    /**
     * To get hive connection url.
     * @return
     */
    public String getHiveConnUrl() {
        if (StringUtils.isBlank(connUrl)) {
            this.connUrl = this.getConfiguration().get(BigdataUtilsGlobalConstants.HIEV_JDBC_URL_KEY);
        }
        return connUrl;
    }

    /**
     * Hive field definition.
     */
    @Data
    public class HiveField {
        private String fieldName;
        private String fieldType;
        private int index;
        private boolean isPartition;
    }
}
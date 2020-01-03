package com.opens.bigdafork.utils.tools.hive.manage;

import com.opens.bigdafork.utils.common.constants.BigdataUtilsGlobalConstants;
import com.opens.bigdafork.utils.common.exceptions.LoadConfigException;
import com.opens.bigdafork.utils.common.util.DefaultConnect4Ready;
import com.opens.bigdafork.utils.tools.AbstractManageUtils;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
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

    private static final String PARTI_KEY = "# Partition Information";
    private static final String COL_KEY = "# col_name";
    private static final String MK_FLAG = "mk";

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

            //all of rsmd getXXX methods are invalid except getColumnCount.
            ResultSetMetaData rsmd = rs.getMetaData();
            //LOGGER.debug(rsmd.getColumnCount() + ""); //result is 3;
            int i = 0;
            String kName;
            while (rs.next()) {
                kName = rs.getString(1);
                if (checkKeyNameIsInvalid(kName)) {
                    continue;
                }
                if (kName.startsWith(PARTI_KEY)) {
                    break;
                }
                HiveField field = new HiveField();
                field.setFieldName(kName);
                field.setFieldType(rs.getString(2));
                field.setIndex(i++);
                if (MK_FLAG.equals(rs.getString(3))) {
                    field.setMK(true);
                }
                fieldsMap.put(field.getFieldName(), field);
            }

            while (rs.next()) {
                kName = rs.getString(1);
                if (checkKeyNameIsInvalid(kName)) {
                    continue;
                }

                HiveField partitionField = fieldsMap.get(kName);
                if (partitionField != null) {
                    partitionField.setPartition(true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("getHiveFieldsOfHiveTable, an exception occurs ： " + e.getMessage());
        }
        return fieldsMap;
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

    @Override
    public Configuration getEnvConfiguration() {
        return new Configuration(this.getConfiguration());
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
        return HiveOpUtils.getConnectionSafeMode(url);
    }

    private boolean checkKeyNameIsInvalid(String kName) {
        return StringUtils.isBlank(kName) || kName.startsWith(COL_KEY);
    }

    /**
     * Hive field definition.
     */
    @Data
    public class HiveField {
        private String fieldName;
        private String fieldType;
        private boolean isMK;
        private int index;
        private boolean isPartition;
    }
}

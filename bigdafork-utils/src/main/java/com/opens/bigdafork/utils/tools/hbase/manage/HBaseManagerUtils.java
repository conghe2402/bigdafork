package com.opens.bigdafork.utils.tools.hbase.manage;

import com.google.common.collect.Lists;
import com.opens.bigdafork.utils.common.util.DefaultConnect4Ready;
import com.opens.bigdafork.utils.tools.AbstractManageUtils;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Every this class`s object holds a environment context as soon as it`s instantiated.
 * And Its environment context can be got via invoking method #getEnvConfiguration.
 *
 * a few of management functions:
 * 1.query detail information of regions belongs to a htable.
 * 2.query a region location list without duplicates.
 * 3.query specified table detail info.
 * 4.count number of regions.
 * 5.count number of region locations.
 * 6.compact operation.
 * 7.return env configuration.
 * 8.check out if a table exist.
 */
public class HBaseManagerUtils extends AbstractManageUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseManagerUtils.class);

    public HBaseManagerUtils() {
        super(null);
    }

    public HBaseManagerUtils(Properties properties) {
        super(properties);
    }

    /**
     * To get table detail info.
     * @param hTableName
     * @return
     */
    public TableInfo getTableInfoOfHTable(String hTableName) {
        LOGGER.debug(String.format("get Table infos of Htable [%s]", hTableName));
        TableInfo tableInfo = new TableInfo(hTableName);
        int regionLocationsNumber = this.getRegionLocationNumberOfHTable(hTableName);
        int regionNumber = this.getRegionNumberOfHTable(hTableName);
        List<RegionDetailInfo> regionDetailInfoList = this.getRegionDetailListOfHTable(hTableName);

        tableInfo.setRegionLocationsNum(regionLocationsNumber);
        tableInfo.setRegionNum(regionNumber);
        tableInfo.setRegionDetailInfoList(regionDetailInfoList);

        return tableInfo;
    }

    /**
     *
     * @param hTableName
     * @return
     */
    public List<RegionDetailInfo> getRegionDetailListOfHTable(String hTableName) {
        LOGGER.debug(String.format("get Region detail list of Htable [%s]", hTableName));
        List<RegionDetailInfo> regionInfoList = Lists.newArrayList();
        try (Connection connection = getConnection()) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(hTableName));
            for (HRegionLocation regionLocation : regionLocator.getAllRegionLocations()) {
                RegionDetailInfo regionDetailInfo = new RegionDetailInfo();
                HRegionInfo hRegionInfo = regionLocation.getRegionInfo();
                regionDetailInfo.setTableName(hTableName);
                regionDetailInfo.setName(hRegionInfo.getRegionNameAsString());
                regionDetailInfo.setStartRowkey(Bytes.toString(hRegionInfo.getStartKey()));
                regionDetailInfo.setEndRowkey(Bytes.toString(hRegionInfo.getEndKey()));
                //regionDetailInfo.setRowNumber(hRegionInfo.);
                //it seems that no api can count row number of every region

                regionDetailInfo.setHostname(regionLocation.getHostname());
                regionDetailInfo.setHostnamePort(regionLocation.getHostnamePort());
                regionDetailInfo.setPort(regionLocation.getPort());
                regionDetailInfo.setSeqNum(regionLocation.getSeqNum());
                regionDetailInfo.setServerName(regionLocation.getServerName().getServerName());
                regionInfoList.add(regionDetailInfo);
            }
        } catch (IOException e) {
            LOGGER.error(String.format("get region number error : ", e.getMessage()));
        }
        return regionInfoList;
    }

    /**
     * To count number of regions.
     * @param hTableName
     * @return
     */
    public int getRegionNumberOfHTable(String hTableName) {
        LOGGER.debug(String.format("get Region number of Htable [%s]", hTableName));
        int number = 0;
        try (Connection connection = getConnection()) {
            Admin admin = connection.getAdmin();
            List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf(hTableName));
            number = regions == null ? number : regions.size();
        } catch (IOException e) {
            LOGGER.error(String.format("get region number error : %s", e.getMessage()));
        }
        return number;
    }

    /**
     * To count number of region locations.
     * @param hTableName
     * @return
     */
    public int getRegionLocationNumberOfHTable(String hTableName) {
        LOGGER.debug(String.format("get Region location number of Htable [%s]", hTableName));
        int countNum = this.getRegionLocationListOfHTable(hTableName).size();
        return countNum;
    }

    /**
     * To query region location identity of htable.
     * @param hTableName
     * @return
     */
    public List<String> getRegionLocationListOfHTable(String hTableName) {
        LOGGER.debug(String.format("get Region location number of Htable [%s]", hTableName));
        List<String> locationList = Lists.newArrayList();
        try (Connection connection = getConnection()) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(hTableName));

            List<HRegionLocation> tempCacheRegionLocation = Lists.newArrayList();
            for (HRegionLocation regionLocation : regionLocator.getAllRegionLocations()) {
                // just judge equivalence as per location`s server name
                if (tempCacheRegionLocation.contains(regionLocation)) {
                    continue;
                }
                tempCacheRegionLocation.add(regionLocation);
                locationList.add(regionLocation.getServerName().toString());
            }
            tempCacheRegionLocation.clear();
        } catch (IOException e) {
            LOGGER.error(String.format("get region location number error : %s", e.getMessage()));
        }
        return locationList;
    }

    /**
     * To get a copy of env configuration object.
     * @return
     */
    public Configuration getEnvConfiguration() {
        return HBaseConfiguration.create(this.getConfiguration());
    }

    /**
     * To check if the table exists.
     * @param hTableName
     * @return
     */
    public boolean checkTableExist(String hTableName) {
        final TableName tName = TableName.valueOf(hTableName);
        try (Connection connection = getConnection()) {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tName)) {
                LOGGER.debug("Table " + tName + " does not exists");
                return false;
            }
        } catch (IOException e) {
            LOGGER.error(String.format("check table exist error : %s", e.getMessage()));
            return false;
        }
        return true;
    }

    @Override
    protected void initialize() {
        this.setConfiguration(HBaseConfiguration.create());
        DefaultConnect4Ready.ready4Hbase(this.getConfiguration());
    }

    private Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(this.getConfiguration());
    }

    /**
     * region details` encapsulation.
     */
    @Data
    public class RegionDetailInfo {
        private String tableName;
        private String name;
        private Long rowNumber;
        private String startRowkey;
        private String endRowkey;
        private String hostname;
        private String hostnamePort;
        private int port;
        private long seqNum;
        private String serverName;
    }

    /**
     * table information`s encapsulation.
     */
    @Data
    public class TableInfo {
        private String tableName;
        private int regionNum;
        private int regionLocationsNum;
        private List<RegionDetailInfo> regionDetailInfoList;

        public TableInfo(String tableName) {
            this.tableName = tableName;
        }
    }
}

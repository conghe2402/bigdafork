package com.opens.tools.hbase.op;

import com.google.protobuf.InvalidProtocolBufferException;
import com.opens.tools.hbase.HBaseConfigBean;
import com.opens.tools.hbase.manage.HBaseManagerUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a few of hbase operate functions.
 * Make sure this class object is non-status.
 *
 * 1.compose a whole scan for spark app.
 * 2.get a connection to hbase server with initial settings and environment configuration.
 * 3.compose a region scan for spark app.
 * 4.get a new scan with some settings.
 */
public final class HBaseOpUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseOpUtils.class);

    private HBaseOpUtils() {}

    /**
     * compose a scan for spark app.
     * if configBean`s useNewConfig attribution is set true,
     * the method will return a clone configuration with a scan
     * and authentication for the purpose of saving resources,
     * or return default inner configuration.
     * @param configBean
     * @param configuration
     * @return
     */
    public static Configuration setWholeScanInfo(HBaseConfigBean configBean, Configuration configuration) {
        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan protoScan = null;
        try {
            Scan scan = newScan(configBean);
            protoScan = ProtobufUtil.toScan(scan);
        } catch (IOException e) {
            LOGGER.error(String.format("protoScan occurs exception : %s", e.getMessage()));
            return null;
        }
        return setScan(protoScan, configBean, configuration);
    }

    /**
     * To set a scam on a region of hTable for spark app.
     * if configBean`s useNewConfig attribution is set true，
     * the method will return a clone configuration with a scan and authentication for the purpose of saving resources,
     * or return default inner configuration
     * @param configBean
     * @param regionDetailInfo
     * @return
     */
    public static Configuration setScanOnRegion(HBaseConfigBean configBean,
                                                HBaseManagerUtils.RegionDetailInfo regionDetailInfo,
                                                Configuration configuration) {
        org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan protoScan;
        try {
            configBean.setTableName(regionDetailInfo.getTableName());
            Scan scan = newScan(configBean);

            LOGGER.debug(String.format("a scan over a region of %s begins with rk : %s",
                    regionDetailInfo.getTableName(),
                    regionDetailInfo.getStartRowkey()));
            LOGGER.debug(String.format("a scan over a region of %s ends at rk : %s",
                    regionDetailInfo.getTableName(),
                    regionDetailInfo.getEndRowkey()));
            scan.setStartRow(regionDetailInfo.getStartRowkey().getBytes());
            scan.setStopRow(regionDetailInfo.getEndRowkey().getBytes());
            protoScan = ProtobufUtil.toScan(scan);
        } catch (IOException e) {
            LOGGER.error(String.format("protoScan occurs exception : %s", e.getMessage()));
            return null;
        }
        return setScan(protoScan, configBean, configuration);
    }

    /**
     * To get Scan instance from specified configuration has set scan.
     * @param configuration
     * @return
     * @throws IOException
     */
    public static Scan getScanInstance(Configuration configuration) throws IOException {
        if (null != configuration.get(TableInputFormat.SCAN)) {
            byte [] decoded = Base64.decode(configuration.get(TableInputFormat.SCAN));
            ClientProtos.Scan protoScan;
            try {
                protoScan = ClientProtos.Scan.parseFrom(decoded);
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error(String.format("getCurrentScanInstance occurs invalid " +
                        "protocol buffer exception : %s", e.getMessage()));
                throw new IOException(e);
            }

            return ProtobufUtil.toScan(protoScan);
        }
        return null;
    }

    /**
     * To get a connection to hbase with customer settings.
     * @param configuration
     * @return
     * @throws IOException
     */
    public static Connection getConnection(Configuration configuration) throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

    private static Scan newScan(HBaseConfigBean configBean) {
        Scan scan = new Scan();
        scan.setCacheBlocks(configBean.isCacheBlock());
        if (configBean.getCacheNum() > 1) {
            scan.setCaching(configBean.getCacheNum());
        }
        if (configBean.getBatchNum() > 1) {
            scan.setBatch(configBean.getBatchNum());
        }
        return scan;
    }

    /**
     * To compose a scan operation.
     * @param protoScan
     * @param configBean
     * @param configuration
     * @return
     */
    private static Configuration setScan(org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan protoScan,
                                  HBaseConfigBean configBean, Configuration configuration) {
        Configuration scanConfig = configuration;
        if (configBean.isUseNewConfig()) {
            scanConfig = HBaseConfiguration.create(configuration);
        }
        scanConfig.set(TableInputFormat.INPUT_TABLE, configBean.getTableName());
        scanConfig.set(TableInputFormat.SCAN, Base64.encodeBytes(protoScan.toByteArray()));

        if (configBean.getClientConnThreadNum() > 0) {
            scanConfig.setInt("hbase.hconnection.threads.max", configBean.getClientConnThreadNum());
            scanConfig.setInt("hbase.hconnection.threads.core", configBean.getClientConnThreadNum());
        }

        if (configBean.getLeasePeriodNum() > 0) {
            scanConfig.setInt("hbase.client.scanner.timeout.period", configBean.getLeasePeriodNum());
        } else {
            scanConfig.setInt("hbase.client.scanner.timeout.period", 120000);
        }
        if (configBean.getBalanceRatioNum() > 0) {
            scanConfig.set(TableInputFormat.MAPREDUCE_INPUT_AUTOBALANCE, "true");
            scanConfig.setInt(TableInputFormat.INPUT_AUTOBALANCE_MAXSKEWRATIO, configBean.getBalanceRatioNum());
        }
        return scanConfig;
    }
}

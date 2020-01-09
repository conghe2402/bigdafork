package com.opens.bigdafork.simulation.hive.build;

import com.opens.bigdafork.simulation.common.Constants;
import com.opens.bigdafork.simulation.hive.ConfigParser;
import com.opens.bigdafork.simulation.hive.build.fieldValue.FieldValueGen;
import com.opens.bigdafork.utils.tools.hive.manage.HiveManageUtils;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import com.opens.bigdafork.utils.tools.shell.ShellUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * make Table data for m table.
 */
public class TableDataBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDataBuilder.class);
    private static final char SEP = ',';
    private static final String DATE = "20200101";
    private static final String NEW_LINE_FLAG = System.getProperty("line.separator");

    private BigDecimal startId = new BigDecimal("580000000000000000");

    private String fileDir = "/root/workspace/bds/";
    private String mTableName = null;
    private Map<String, HiveManageUtils.HiveField> fieldsMap = null;
    private long rolNumber = 0;
    private String datFileName = "";
    private Random rm = new Random();
    private Configuration env;
    private boolean hasPartition = false;
    private String partitionFieldName = "";
    private String filePath = "";
    private String destDfsHomePath = "/user/test/";
    private String destFilePath = "";
    private String tmpMTableName = "";
    private NumberFormat numberFormat;
    //special field value generator.
    private Map<String, FieldValueGen> fvgMap = new HashMap<>();

    public TableDataBuilder(Configuration env, String mTableName,
                            Map<String, HiveManageUtils.HiveField> fieldsMap,
                            long rowNumber) {
        this.env = env;
        this.mTableName = mTableName;
        this.tmpMTableName = mTableName + Constants.TEMP_TABLE_SUFFIX;
        this.fieldsMap = fieldsMap;
        this.datFileName = String.format("%s.txt", mTableName);
        this.filePath = fileDir + datFileName;
        this.rolNumber = rowNumber;
        checkIsPartitionTable();
        numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(0);
        numberFormat.setGroupingUsed(false);
        setFieldValueGen();
    }

    public void outputDataFile() {
        LOGGER.info(String.format("begin to generate data file %s for %s",
                datFileName, mTableName));
        if (fieldsMap == null || fieldsMap.size() <= 0) {
            LOGGER.info("no fields so do not generate data file!");
            return;
        }

        File bdf = new File(filePath);
        if (bdf.isFile() && bdf.exists()) {
            LOGGER.info(String.format("%s exists.", filePath));
            return;
        }
        try (RandomAccessFile acf = new RandomAccessFile(bdf, "rw");
            FileChannel fc = acf.getChannel()) {
            int i = 0;
            int lineMaxLength = getLineMaxLength();
            LOGGER.info("lineMaxLength ： " + lineMaxLength);
            int batchMaxNum = rolNumber > 10000 ? 10000: (int)rolNumber;
            final int batchMaxLen = lineMaxLength * batchMaxNum;
            LOGGER.info("batchMaxLen ： " + batchMaxLen);
            long offset = 0;
            String line;
            byte[] bs;
            while (i < rolNumber) {
                MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, offset, batchMaxLen);
                int batchLen = 0;
                for(int j = 0; j < batchMaxNum; j++, i++) {
                    line = genLine();
                    bs = line.getBytes();
                    if (batchLen + bs.length > batchMaxLen) {
                        break;
                    }
                    batchLen += bs.length;
                    //LOGGER.info(""+i + " - " + line);
                    mbuf.put(bs);
                }
                offset += batchLen;
            }

            LOGGER.info(String.format("done with generating data file %s for %s",
                    datFileName, mTableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void insert2MTable() {
        LOGGER.info(String.format("begin to insert %s into hive table %s",
                datFileName, mTableName));
        String insertSQL;
        if (this.hasPartition) {
            String fields = getFieldsString();
            insertSQL = String.format("insert overwrite table %s partition(%s='%s') select %s from %s",
                    mTableName, partitionFieldName, DATE, fields, tmpMTableName);
        } else {
            insertSQL = String.format("insert overwrite table %s select * from %s",
                    mTableName, tmpMTableName);
        }
        LOGGER.info(insertSQL);
        HiveOpUtils.execDDL(this.env, insertSQL);
        LOGGER.info(String.format("done with inserting %s into hive table %s",
                datFileName, mTableName));
    }

    /**
     * load data into temp table from HDFS.
     */
    public void loadHDFSData2TmpTable() {
        LOGGER.info(String.format("begin to load %s into hive table %s",
                datFileName, tmpMTableName));
        String loadSql;
        if (this.hasPartition) {
            loadSql = String.format("load data inpath '%s' overwrite into table %s partition(%s='%s')",
                    this.destFilePath, this.tmpMTableName, this.partitionFieldName, DATE);
        } else {
            loadSql = String.format("load data inpath '%s' overwrite into table %s",
                    this.destFilePath, this.tmpMTableName);
        }
        HiveOpUtils.execDDL(this.env, loadSql);
        LOGGER.info(String.format("done with loading data %s into hive table %s",
                datFileName, tmpMTableName));
    }

    public boolean putIntoHDFS() {
        LOGGER.info(String.format("begin to put %s into hfds dir %s.", filePath, destDfsHomePath));
        String cmd = String.format("sh /root/workspace/bigdafork-simulation-test-1.0-SNAPSHOT/upload %s %s",
                this.filePath, this.destDfsHomePath);
        try {
            ShellUtil.execCommand(cmd);
            this.destFilePath = this.destDfsHomePath +datFileName;
            LOGGER.info("done with put into hdfs");
            return true;
        } catch (Exception e) {
            LOGGER.error("put into hdfs fail : " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * only support single partition whose format is YYYYMMDD and with regular value "20200101".
     * @return
     */
    private String genLine() {
        StringBuilder line = new StringBuilder();
        for (Map.Entry<String, HiveManageUtils.HiveField> fieldEntry : fieldsMap.entrySet()) {
            HiveManageUtils.HiveField field = fieldEntry.getValue();
            if (field.isMK()){
                String id = numberFormat.format(startId);
                line.append(id).append(SEP);
            } else if (field.isPartition()) {
                line.append(DATE);
            } else {
                String value;
                FieldValueGen fvg = fvgMap.get(field.getFieldName());
                if (fvg != null && fvg.getTableName().equals(this.mTableName)
                        && fvg.getFieldName().equals(field.getFieldName())) {
                    value = fvg.getValue();
                } else {
                    value = String.format("%03d", (rm.nextInt(100) + 1));
                }
                line.append(value).append(SEP);
            }
        }
        if (line.length() > 0 && line.charAt(line.length() - 1) == SEP) {
            line.deleteCharAt(line.length() - 1);
        }
        line.append(NEW_LINE_FLAG);
        startId = startId.add(new BigDecimal(1));
        return line.toString();
    }

    private int getLineMaxLength() {
        int length = 0;
        for (Map.Entry<String, HiveManageUtils.HiveField> fieldEntry : fieldsMap.entrySet()) {
            HiveManageUtils.HiveField field = fieldEntry.getValue();

            if (field.isMK()){
                length += startId.toString().length();
            } else if (field.isPartition()) {
                length += DATE.length();
            } else {
                FieldValueGen fvg = fvgMap.get(field.getFieldName());
                if (fvg != null && fvg.getTableName().equals(this.mTableName)
                        && fvg.getFieldName().equals(field.getFieldName())) {
                    length += fvg.getMaxValueLength();
                } else {
                    length += 3;
                }
            }
            length++;
        }

        return length;
    }

    private String getFieldsString() {
        StringBuilder fields = new StringBuilder();
        for (HiveManageUtils.HiveField field : this.fieldsMap.values()) {
            if (field.isPartition()) {
                continue;
            }
            fields.append(field.getFieldName()).append(",");
        }

        if (fields.length() > 0) {
            fields.deleteCharAt(fields.length() - 1);
        }

        return fields.toString();
    }

    private void setFieldValueGen() {
        for (Map.Entry<String, HiveManageUtils.HiveField> fieldEntry : fieldsMap.entrySet()) {
            String comment = fieldEntry.getValue().getComment();
            if (StringUtils.isBlank(comment)) {
                continue;
            }
            String fn = fieldEntry.getValue().getFieldName();
            FieldValueGen fvg = (FieldValueGen)ConfigParser.parseItem(this.mTableName, fn, comment);
            if (fvg != null) {
                this.fvgMap.put(fn, fvg);
            }
        }
    }

    private boolean checkIsPartitionTable() {
        for (Map.Entry<String, HiveManageUtils.HiveField> fieldEntry : fieldsMap.entrySet()) {
            if (fieldEntry.getValue().isPartition()) {
                this.hasPartition = true;
                this.partitionFieldName = fieldEntry.getKey();
                LOGGER.info("this table has partition field : " + fieldEntry.getKey());
                return true;
            }
        }
        this.hasPartition = false;
        return false;
    }
}

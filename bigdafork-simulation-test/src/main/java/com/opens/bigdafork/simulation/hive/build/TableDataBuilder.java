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
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.*;
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
    private static final int MAX_FILE_SIZE = 60;
    private BigDecimal startId = new BigDecimal("580000000000000000");

    private String fileDir = "/srv/BigData/storm/bds/";
    private String mTableName = null;
    private Map<String, HiveManageUtils.HiveField> fieldsMap = null;
    private long rolNumber = 0;
    private String datFileName = "";
    private int lineMaxLength;
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
        File fileDirFile = new File(fileDir);
        if (!fileDirFile.exists()) {
            fileDirFile.mkdir();
        }

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

        lineMaxLength = getLineMaxLength();
        LOGGER.info("lineMaxLength ： " + lineMaxLength);
        long fileSize = this.rolNumber * lineMaxLength;
        if (fileSize/1024/1024/1024 > MAX_FILE_SIZE) {
            LOGGER.info("Normal write mode!");
            this.writeBuff();
            //this.write();
        } else {
            //LOGGER.info("MMC write mode!");
            this.write();
        }
        LOGGER.info(String.format("start to output data to file : %s", filePath));

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
            this.destFilePath = this.destDfsHomePath + datFileName;
            LOGGER.info("done with put into hdfs");

            File datFile = new File(filePath);
            /*
            if (datFile.exists()) {
                if (datFile.delete()) {
                    LOGGER.info(String.format("delete file Success ：%s", filePath));
                } else {
                    LOGGER.info(String.format("delete file Fail ：%s", filePath));
                }
            }
            */
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
                    value = String.valueOf(rm.nextInt(10));
                    //value = String.format("%03d", (rm.nextInt(10) + 1));
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

    private void write() {
        //int batchNum = (int)(this.rolNumber > 10000 ? 10000 : this.rolNumber);
        //int cacheSize = lineMaxLength * batchNum;
        FileWriter writer = null;
        try {
            writer = new FileWriter(this.filePath);
            int i = 1;
            while (i <= rolNumber) {
                String line = genLine();
                writer.write(line);
                i++;
            }
        } catch (IOException e) {
            LOGGER.error("when generating data file, err occurs : " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void writeBuff() {
        int batchNum = (int)(this.rolNumber > 10000 ? 10000 : this.rolNumber);
        int cacheSize = lineMaxLength * batchNum;
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(this.filePath), "UTF-8"), cacheSize)) {
            int i = 1;
            while (i <= rolNumber) {
                String line = genLine();
                bw.append(line);
                if (i++ >= batchNum) {
                    bw.flush();
                }
            }
            bw.flush();
        } catch (IOException e) {
            LOGGER.error("when generating data file, err occurs : " + e.getMessage());
            e.printStackTrace();
        }

    }

    /**
     * this method has a bug that generate file has many illegal data.
     * @^@^@^
     */
    private void mmc() {
        File bdf = new File(filePath);
        MappedByteBuffer mBuf = null;
        try (RandomAccessFile acf = new RandomAccessFile(bdf, "rw");
             FileChannel fc = acf.getChannel()) {
            int i = 0;
            int batchMaxNum = rolNumber > 1000000 ? 1000000: (int)rolNumber;
            //the batchMaxLen can not exceed 2147483647
            final long batchMaxLen = (long)lineMaxLength * batchMaxNum;
            LOGGER.info("batchMaxLen ： " + batchMaxLen);
            long offset = 0;
            String line;
            byte[] bs;
            while (i < rolNumber) {
                //FileChannelImpl
                mBuf = fc.map(FileChannel.MapMode.READ_WRITE, offset, batchMaxLen);
                long batchLen = 0;
                for(int j = 0; j < batchMaxNum; j++, i++) {
                    line = genLine();
                    bs = line.getBytes();
                    if (batchLen + bs.length > batchMaxLen) {
                        startId = startId.add(new BigDecimal(-1));
                        break;
                    }
                    batchLen += bs.length;
                    //LOGGER.info(""+i + " - " + line);
                    mBuf.put(bs);
                }
                offset += batchLen;
                // TODO: 2020/1/10 : manually free pc via /proc/sys/vm/drops_caches
            }

            mBuf.force();
            LOGGER.info(String.format("done with generating data file %s for %s",
                    datFileName, mTableName));
            //this.unmap(mBuf);// IT DOES NOT WORK
        } catch (IOException e) {
            LOGGER.error("when generating data file, err occurs : " + e.getMessage());
            e.printStackTrace();
        }
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

    private void unmap(MappedByteBuffer var0) {
        if (var0 == null) {
            return;
        }
        Cleaner var1 = ((DirectBuffer)var0).cleaner();
        if (var1 != null) {
            LOGGER.info("clean...");
            var1.clean();
            System.gc();
        }
    }
}

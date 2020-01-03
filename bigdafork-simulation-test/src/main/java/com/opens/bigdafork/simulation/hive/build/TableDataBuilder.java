package com.opens.bigdafork.simulation.hive.build;

import com.opens.bigdafork.utils.tools.hive.manage.HiveManageUtils;
import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;

/**
 * Table data.
 */
public class TableDataBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDataBuilder.class);
    private static final char SEP = ',';
    private static final String DATE = "20200101";


    private double START_ID = 580000000000000000d;

    private String fileDir = "";
    private String tableName = null;
    private Map<String, HiveManageUtils.HiveField> fieldsMap = null;
    private int rolNumber = 0;
    private String datFileName = "";
    private Random rm = new Random();

    public TableDataBuilder(String tableName,
                            Map<String, HiveManageUtils.HiveField> fieldsMap,
                            int rowNumber) {
        this.tableName = tableName;
        this.fieldsMap = fieldsMap;
        this.datFileName = String.format("%s%s.bd", tableName);
        this.rolNumber = rolNumber;
    }

    public void outputDataFile() {
        LOGGER.info(String.format("begin to generate data file %s for %s",
                datFileName, tableName));
        File bdf = new File(fileDir + datFileName);
        try (RandomAccessFile acf = new RandomAccessFile(bdf, "rw");
            FileChannel fc = acf.getChannel()) {
            int i = 0;
            String line = genLine();
            byte[] bs = line.getBytes();
            int len = bs.length * 1000;
            long offset = 0;

            while (i < rolNumber) {
                MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_WRITE, offset, len);
                for(int j = 0; j < 1000; j++,i++) {
                    line = genLine();
                    bs = line.getBytes();
                    mbuf.put(bs);
                }
                offset = offset + len;
            }
            LOGGER.info(String.format("done with generating data file %s for %s",
                    datFileName, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void loadLocalData() {
        LOGGER.info(String.format("begin to load local data %s into hive table %s",
                datFileName, tableName));
        String loadSql = String.format("load data local inpath '%s' overwrite into table %s",
                this.datFileName, this.tableName);
        LOGGER.info(loadSql);
        try (Connection connection = HiveOpUtils.getConnection()) {
            HiveOpUtils.execDDL(connection, loadSql);
            LOGGER.info(String.format("done with loading local data %s into hive table %s",
                    datFileName, tableName));
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public void putIntoHDFS() {

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
                String id = String.valueOf(START_ID);
                id = id.substring(0, id.indexOf('.'));
                line.append(id).append(SEP);
            } else if (field.isPartition()) {
                line.append(DATE);
            } else {
                line.append(rm.nextInt(100) + 1).append(SEP);
            }
            START_ID += 1;
        }
        if (line.charAt(line.length() - 1) == SEP) {
            line.deleteCharAt(line.length() - 1);
        }
        return line.toString();
    }
}

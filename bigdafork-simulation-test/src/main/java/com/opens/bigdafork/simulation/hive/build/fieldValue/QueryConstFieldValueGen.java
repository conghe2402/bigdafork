package com.opens.bigdafork.simulation.hive.build.fieldValue;

import com.opens.bigdafork.utils.tools.hive.op.HiveOpUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

/**
 * generate a set of constant values queried from other hive table`s field.
 */
public class QueryConstFieldValueGen extends FieldValueGen {
    private String sourceTable;
    private String sourceField;
    private int type;
    private String[] valueRange;
    private int valueRangeCount;
    private Random rm = new Random();
    private Configuration env;

    public QueryConstFieldValueGen(Configuration env, String tableName, String fieldName,
                              int length, String sourceTable, String sourceField) {
        super(tableName, fieldName, length);
        this.sourceTable = sourceTable;
        this.sourceField = sourceField;
        this.env = env;
    }

    @Override
    public String getValue() {
        return valueRange[rm.nextInt(valueRangeCount)];
    }

    private void queryFieldValueRange() {

        try (Connection connection = HiveOpUtils.getConnection(env)) {
            HiveOpUtils.execQuery(connection, "select ");

        } catch (SQLException | InstantiationException
                | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*
    public static void main(String[] args) {
        ConstFieldValueGen cfvg = new ConstFieldValueGen("1", "2", new String[]{"01","02"});
        for (int i=0;i<100;i++) {
            System.out.println(cfvg.getValue());
        }

    }
    */
}

package com.opens.bigdafork.simulation.hive.build.fieldValue;

import lombok.Getter;
import java.util.Random;

/**
 * generate field value.
 */
public abstract class FieldValueGen {
    @Getter
    private String fieldName;
    @Getter
    private String tableName;
    @Getter
    private int length;
    @Getter
    private int type;

    protected Random rm = new Random();

    public FieldValueGen(String tableName, String fieldName,
                         int length) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.length = length;
    }

    public abstract String getValue();

    public int getMaxValueLength() {
        return this.getLength();
    }

    protected String getConstLengthValue(String value) {
        StringBuilder sb = new StringBuilder();
        int addLength = this.getMaxValueLength() - value.length();
        int i = 0;
        while (i++ < addLength) {
            sb.append(" ");
        }
        sb.append(value);
        return sb.toString();
    }

}

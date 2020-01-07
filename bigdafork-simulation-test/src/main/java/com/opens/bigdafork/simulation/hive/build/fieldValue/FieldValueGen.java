package com.opens.bigdafork.simulation.hive.build.fieldValue;

import lombok.Getter;
import java.util.Random;

/**
 * generate field value.
 */
public class FieldValueGen {
    @Getter
    private String fieldName;
    @Getter
    private String tableName;
    private Random rm = new Random();

    public FieldValueGen(String tableName, String fieldName) {
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    public String getValue() {
        return String.valueOf(rm.nextInt(128) + 1);
    }
}

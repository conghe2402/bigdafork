package com.opens.bigdafork.simulation.hive.build.fieldValue;

/**
 * RandomFieldValueGen.
 */
public class RandomFieldValueGen extends FieldValueGen {
    private int seed;
    private int offset;

    public RandomFieldValueGen(String tableName, String fieldName,
                               int length, int down, int up) {
        super(tableName, fieldName, length);
        int upLimit = Math.max(up, down);
        int downLimit = Math.min(up, down);
        this.seed = upLimit - downLimit;
        this.offset = downLimit;

    }

    @Override
    public String getValue() {
        int value = (int)((Math.random() * seed) + offset);
        return getConstLengthValue(String.valueOf(value));
    }
}

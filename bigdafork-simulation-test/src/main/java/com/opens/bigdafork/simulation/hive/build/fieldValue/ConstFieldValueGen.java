package com.opens.bigdafork.simulation.hive.build.fieldValue;

/**
 * generate constant value.
 */
public class ConstFieldValueGen extends FieldValueGen {
    private String[] valueRange;
    private int valueRangeCount;

    public ConstFieldValueGen(String tableName, String fieldName,
                              int length, String[] valueRange) {
        super(tableName, fieldName, length);
        this.valueRange = valueRange;
        this.valueRangeCount = valueRange.length;
    }

    @Override
    public String getValue() {
        return this.getConstLengthValue(valueRange[rm.nextInt(valueRangeCount)]);
    }

}

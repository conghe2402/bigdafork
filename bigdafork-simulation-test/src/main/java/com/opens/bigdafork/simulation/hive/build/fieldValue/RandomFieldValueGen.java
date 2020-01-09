package com.opens.bigdafork.simulation.hive.build.fieldValue;

/**
 * RandomFieldValueGen.
 */
public class RandomFieldValueGen extends FieldValueGen {

    private int randSeed;

    public RandomFieldValueGen(String tableName, String fieldName,
                               int length, int randSeed) {
        super(tableName, fieldName, length);
        this.randSeed = randSeed;
    }

    @Override
    public String getValue() {
        return getConstLengthValue(String.valueOf(rm.nextInt(randSeed)));
    }
}

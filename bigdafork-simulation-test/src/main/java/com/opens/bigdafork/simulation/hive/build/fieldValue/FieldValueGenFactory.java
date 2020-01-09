package com.opens.bigdafork.simulation.hive.build.fieldValue;

import java.util.Map;

/**
 * FieldValueGenFactory.
 */
public final class FieldValueGenFactory {

    public static FieldValueGen getOne(Map<String, String> params) {
        if ("1".equals(params.get("choice"))) {
            return  null;
        }
        return null;
    }

    private FieldValueGenFactory() {}
}

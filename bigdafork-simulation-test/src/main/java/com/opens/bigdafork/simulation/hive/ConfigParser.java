package com.opens.bigdafork.simulation.hive;

import com.opens.bigdafork.simulation.common.Constants;
import com.opens.bigdafork.simulation.hive.build.fieldValue.ConstFieldValueGen;
import com.opens.bigdafork.simulation.hive.build.fieldValue.RandomFieldValueGen;
import org.apache.commons.lang.StringUtils;

/**
 * Config parser.
 */
public final class ConfigParser {

    public static Object parseItem(String mTableName, String fieldName, String item) {
        if (StringUtils.isBlank(item)) {
            return null;
        }

        //field value generator
        if (item.startsWith(Constants.FIELD_VALUE_GEN)) {
            String[] arr = item.split(Constants.FIELD_VALUE_GEN_SEP);
            if (arr.length < 4) {
                return null;
            }
            String[] constArr = arr[3].split(Constants.FIELD_VALUE_GEN_CONST_SEP);
            //can be reconstructed to reflect out gen obj with common json params.
            if (arr[1].equals("1")) {
                return new RandomFieldValueGen(mTableName, fieldName,
                        Integer.parseInt(arr[2]),
                        Integer.parseInt(constArr[0]), Integer.parseInt(constArr[1]));
            } else if (arr[1].equals("2")) {

                return new ConstFieldValueGen(mTableName, fieldName,
                        Integer.parseInt(arr[2]), constArr);
            }
        }

        return null;
    }


    private ConfigParser() {}

}

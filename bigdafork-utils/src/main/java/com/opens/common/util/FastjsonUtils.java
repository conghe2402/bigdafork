package com.opens.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.List;

/**
 * To support transformation between json string and object.
 */
public final class FastjsonUtils {
    private static final SerializerFeature[] FEATURES_WITH_NULL_VALUE = {SerializerFeature.WriteMapNullValue,
        SerializerFeature.WriteNullBooleanAsFalse, SerializerFeature.WriteNullListAsEmpty,
        SerializerFeature.WriteNullNumberAsZero, SerializerFeature.WriteNullStringAsEmpty};

    private FastjsonUtils() {}

    public static <T> T convertJSONToObject(String data, Class<T> clzss) {
        try {
            T t = JSON.parseObject(data, clzss);
            return t;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T> List<T> convertJSONToArray(String data, Class<T> clzss) {
        try {
            List<T> t = JSON.parseArray(data, clzss);
            return t;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String convertObjectToJSON(Object obj) {
        try {
            String text = JSON.toJSONString(obj);
            return text;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String convertObjectToJSONWithNullValue(Object obj) {
        try {
            String text = JSON.toJSONString(obj, FEATURES_WITH_NULL_VALUE);
            return text;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}

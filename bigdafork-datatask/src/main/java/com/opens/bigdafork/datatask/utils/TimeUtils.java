package com.opens.bigdafork.datatask.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * TimeUtils.
 */
public final class TimeUtils {

    /**
     * getTimeDiffFormat.
     * @param start
     * @param end
     * @return
     */
    public static String getTimeDiffFormat(long start, long end) {
        Long result = end - start;    //
        long nd = 1000 * 24 * 60 * 60;
        long nh = 1000 * 60 * 60;
        long nm = 1000 * 60;
        long hour = result % nd / nh;     //
        long min = result % nd % nh / nm;  //
        long day = result / nd;

        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm");
        long hMiles = hour * 3600000;  //
        long mMiles = min * 60000;    //
        long resulMiles = (hMiles + mMiles);

        formatter.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
        String resultFormat = formatter.format(resulMiles);
        return resultFormat + "," + day;
    }

    public static String getSimpleDate(long time) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date(time));
    }

    public static String getSimpleDate2(long time) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        return simpleDateFormat.format(new Date(time));
    }

    private TimeUtils() {

    }
}

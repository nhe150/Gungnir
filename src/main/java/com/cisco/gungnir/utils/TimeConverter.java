package com.cisco.gungnir.utils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class TimeConverter implements Serializable {
    public static List<String> fromFormatStrings = Arrays.asList("yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private SimpleDateFormat toFormat;

    public TimeConverter() {};

    public TimeConverter(String toPattern) {
        this.toFormat = new SimpleDateFormat(toPattern);
        toFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public String convert(String timeStamp) throws Exception {
        for (String formatString : fromFormatStrings) {
            try {
                SimpleDateFormat fromFormat = new SimpleDateFormat(formatString);
                fromFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                return toFormat.format(fromFormat.parse(timeStamp));
            } catch (Exception e) {

            }
        }

        System.err.println("Can't parse timestamp string:" + timeStamp + ", use current timestamp");
        return toFormat.format(new Timestamp(System.currentTimeMillis()));

    }

    public Timestamp toTimestamp(String timeStamp) throws Exception {
        for (String formatString : fromFormatStrings) {
            try {
                SimpleDateFormat fromFormat = new SimpleDateFormat(formatString);
                fromFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                return new Timestamp(fromFormat.parse(timeStamp).getTime());
            } catch (Exception e) {
            }
        }

        System.err.println("Can't parse timestamp string:" + timeStamp + ", use current timestamp");
        return new Timestamp(System.currentTimeMillis());

    }

    public String convert(Timestamp timeStamp) throws Exception {
        try {
            return toFormat.format(timeStamp);
        } catch (Exception e) {

        }

        System.err.println("Can't parse timestamp string:" + timeStamp + ", use current timestamp");
        return toFormat.format(new Timestamp(System.currentTimeMillis()));
    }
}
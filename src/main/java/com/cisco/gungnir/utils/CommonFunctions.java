package com.cisco.gungnir.utils;

import org.apache.spark.sql.Dataset;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommonFunctions implements Serializable {
    public static boolean hasColumn(Dataset dataset, String columnName){
        for(String column: dataset.columns()){
            if(columnName.equals(column)){
                return true;
            }
        }
        return false;
    }

    public static Set<String> getPeriodStartDateList(String startDate, String endDate, String period) throws Exception{
        Set<String> dates = new HashSet<>();
        for(String date: getDaysBetweenDates(startDate, endDate)){
            dates.add(getPeriodStartDate(date, period));
        }
        return dates;
    }

    public static List<String> getDaysBetweenDates(String startDate, String endDate) throws Exception{
        LocalDate start = LocalDate.parse(startDate);
        LocalDate end = LocalDate.parse(endDate);
        List<String> totalDates = new ArrayList<>();
        while (!start.isAfter(end)) {
            totalDates.add(start.toString("yyyy-MM-dd"));
            start = start.plusDays(1);
        }

        return totalDates;
    }

    public static String getPeriodStartDate(String otherDate, String period) throws Exception {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final DateTime dateTime = new DateTime(format.parse(otherDate));
        if ("weekly".equals(period)) return getFirstDayOfWeek(dateTime).toString("yyyy-MM-dd");
        if ("monthly".equals(period)) return dateTime.withDayOfMonth(1).toString("yyyy-MM-dd");
        return otherDate;
    }

    public static DateTime getFirstDayOfWeek(DateTime other) {
        if (other.getDayOfWeek() == 7)
            return other;
        else
            return other.minusWeeks(1).withDayOfWeek(7);
    }

    public static List<String> aggregateDates(String startDate, String period) throws Exception {
        List<String> dates = new ArrayList<>();
        dates.add(startDate);
        if (period == null) return dates;
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = format.parse(startDate);
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, date.getYear());
        calendar.set(Calendar.MONTH, date.getMonth());

        int aggregateDuration = 1;
        if ("weekly".equals(period)) aggregateDuration = 7;
        if ("monthly".equals(period)) aggregateDuration = calendar.getActualMaximum(Calendar.DATE);

        calendar.setTime(date);

        for (int i = 2; i <= aggregateDuration; i++) {
            calendar.add(Calendar.DAY_OF_YEAR, 1);
            dates.add(format.format(calendar.getTime()));
        }

        return dates;
    }

    public static class TimeConverter implements Serializable {
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
}

package com.cisco.gungnir.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.*;

public class Aggregation implements Serializable {
    private String watermarkDelayThreshold = "1800 minutes";
    private String aggregationPeriod = "daily";

    public Aggregation(String period){
        setAggregatePeriod(period);
    }

    public void setAggregatePeriod(String period){
        if(period == null) return;
        switch (period){
            case "daily":
                setWatermarkDelayThreshold("4 hours");
                break;
            case "weekly":
                setWatermarkDelayThreshold("8 days");
                break;
            case "monthly":
                setWatermarkDelayThreshold("32 days");
                break;
            default:
                throw new IllegalArgumentException("Invalid input for aggregation period");
        }
        aggregationPeriod = period;
    }

    public String getAggregatePeriod(){
        return this.aggregationPeriod;
    }

    public void registerAggregationFunctions(SparkSession spark){
        spark.udf().register("periodTag", new PeriodTag(aggregationPeriod), DataTypes.StringType);
        spark.udf().register("aggregateStartDate", new AggregateStartDate(aggregationPeriod), DataTypes.TimestampType);
    }

    public void setWatermarkDelayThreshold(String threshold){
        this.watermarkDelayThreshold = threshold;
    }

    public String getWatermarkDelayThreshold(){
        return this.watermarkDelayThreshold;
    }



    public static Set<String> getPeriodStartDateList(String startDate, String endDate, String period) throws Exception{
        Set<String> dates = new HashSet<>();
        for(String date: DateUtil.getDaysBetweenDates(startDate, endDate)){
            dates.add(getPeriodStartDate(date, period));
        }
        return dates;
    }



    public static String getPeriodStartDate(String otherDate, String period) throws Exception {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final DateTime dateTime = new DateTime(format.parse(otherDate));
        if ("weekly".equals(period)) return getFirstDayOfWeek(dateTime).toString("yyyy-MM-dd");
        if ("monthly".equals(period)) return dateTime.withDayOfMonth(1).toString("yyyy-MM-dd");
        return otherDate;
    }

    public static DateTime getFirstDayOfWeek(DateTime other) {
        if (other.getDayOfWeek() == DayOfWeek.SUNDAY.getValue())
            return other;
        else
            return other.minusWeeks(1).withDayOfWeek(DayOfWeek.SUNDAY.getValue());
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


    private static class AggregateStartDate implements UDF1<Timestamp, Timestamp> {
        private String period;
        public AggregateStartDate(String period){
            this.period = period;
        }
        public Timestamp call(Timestamp timeStamp) throws Exception {
            DateTime dateTime = new DateTime(timeStamp);
            switch (period){
                case "weekly":
                    return new Timestamp(getFirstDayOfWeek(dateTime).withMillisOfDay(0).getMillis());
                case "monthly":
                    return new Timestamp(dateTime.withDayOfMonth(1).withMillisOfDay(0).getMillis());
                default:
                    return new Timestamp(dateTime.withMillisOfDay(0).getMillis());
            }
        }
    }

    private static class PeriodTag implements UDF1<String, String> {
        private String period;
        public PeriodTag(String period){
            this.period = period;
        }
        public String call(String dummy) throws Exception {
            return period;
        }
    }

}

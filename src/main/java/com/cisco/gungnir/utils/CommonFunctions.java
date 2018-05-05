package com.cisco.gungnir.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommonFunctions implements Serializable {
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

        public TimeConverter() {
        }

        ;

        public TimeConverter(String toPattern) {
            this.toFormat = new SimpleDateFormat(toPattern);
        }

        public String convert(String timeStamp) throws Exception {
            for (String formatString : fromFormatStrings) {
                try {
                    return toFormat.format(new SimpleDateFormat(formatString).parse(timeStamp));
                } catch (Exception e) {

                }
            }

            System.err.println("Can't parse timestamp string:" + timeStamp);
            return PreProcess.BAD_DATA_LABLE;

        }

        public Timestamp toTimestamp(String timeStamp) throws Exception {
            for (String formatString : fromFormatStrings) {
                try {
                    return new Timestamp(new SimpleDateFormat(formatString).parse(timeStamp).getTime());
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
            return PreProcess.BAD_DATA_LABLE;
        }
    }

    public static final class AppFilter implements FilterFunction<String> {
        private String tag;

        public AppFilter(String appName) {
            this.tag = "appname\":" + '"' + appName + '"';
        }

        public boolean call(String line) {
            return line.contains(tag);
        }
    }

    public static final class PreProcess implements FlatMapFunction<String, Tuple2<String, String>> {
        private transient ObjectMapper objectMapper;
        private transient TimeConverter timeConverter;
        public static final String BAD_DATA_LABLE = "BadData";

        public Iterator<Tuple2<String, String>> call(String value) throws Exception {
            List<Tuple2<String, String>> out = new ArrayList<>();
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            if (timeConverter == null) {
                timeConverter = new TimeConverter("yyyy-MM-dd");
            }
            try {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                JsonNode appname = objectNode.get("appname");
                JsonNode _appname = objectNode.get("_appname");
                if (_appname != null && "metrics".equals(_appname.asText())) {
                    return out.iterator();
                } else if (appname != null && "metrics".equals(appname.asText())) {
                    String timestamp = objectNode.get("timeRcvd").asText();
                    out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
                } else {
                    String message = objectNode.get("@message").asText();
                    String timestamp = objectNode.get("@timestamp").asText();
                    String[] jsonMessages = message.split(":", 2);
                    if (jsonMessages.length == 2) {
                        JsonNode metric = objectMapper.readTree(jsonMessages[1].trim());
                        objectNode.set("SM", metric);
                        objectNode.remove("@message");
                        out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
                    } else {
                        out.add(new Tuple2(BAD_DATA_LABLE, value));
                    }
                }
            } catch (Exception e) {
                out.add(new Tuple2(BAD_DATA_LABLE, value));
            }
            return out.iterator();
        }
    }
}

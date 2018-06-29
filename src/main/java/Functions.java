import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class Functions {
    public static final class AppFilter implements FilterFunction<String> {
        private String tag;
        public AppFilter(String appName){
            this.tag = "appname\":" + '"' + appName + '"';
        }

        public boolean call(String line){
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
            try{
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                JsonNode appname = objectNode.get("appname");
                JsonNode _appname = objectNode.get("_appname");
                if(appname!=null){
                    String timestamp = objectNode.get("timeRcvd").asText();
                    out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
                } else if(_appname!=null){
                    String message = objectNode.get("@message").asText();
                    String timestamp = objectNode.get("@timestamp").asText();
                    String[] jsonMessages = message.split(":", 2);
                    if(jsonMessages.length == 2){
                        JsonNode metric = objectMapper.readTree(jsonMessages[1].trim());
                        objectNode.set("SM", metric);
                        objectNode.remove("@message");
                        out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
                    } else {
                        out.add(new Tuple2(BAD_DATA_LABLE, value));
                    }
                } else{
                    out.add(new Tuple2(BAD_DATA_LABLE, value));
                }
            } catch (Exception e){
                out.add(new Tuple2(BAD_DATA_LABLE, value));
            }
            return out.iterator();
        }
    }

    public static class TimeConverter implements Serializable {
        public static List<String> fromFormatStrings = Arrays.asList("yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        private SimpleDateFormat toFormat;

        public TimeConverter(){};
        public TimeConverter(String toPattern){
            this.toFormat = new SimpleDateFormat(toPattern);
        }

        public String convert(String timeStamp) throws Exception {
            for (String formatString : fromFormatStrings)
            {
                try
                {
                    return toFormat.format(new SimpleDateFormat(formatString).parse(timeStamp));
                }
                catch (Exception e) {

                }
            }

            System.err.println("Can't parse timestamp string:" + timeStamp);
            return Functions.PreProcess.BAD_DATA_LABLE;

        }

        public Timestamp toTimestamp(String timeStamp) throws Exception {
            for (String formatString : fromFormatStrings)
            {
                try
                {
                    return new Timestamp(new SimpleDateFormat(formatString).parse(timeStamp).getTime());
                }
                catch (Exception e) {
                }
            }

            System.err.println("Can't parse timestamp string:" + timeStamp + ", use current timestamp");
            return new Timestamp(System.currentTimeMillis());

        }

        public String convert(Timestamp timeStamp) throws Exception {
            try{
                return toFormat.format(timeStamp);
            } catch (Exception e) {

            }
            return Functions.PreProcess.BAD_DATA_LABLE;
        }
    }
}

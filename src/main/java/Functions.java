import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

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
        public static final String BAD_DATA_LABLE = "BAD_DATA";

        public Iterator<Tuple2<String, String>> call(String value) throws Exception {
            List<Tuple2<String, String>> out = new ArrayList<>();
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            if (timeConverter == null) {
                timeConverter = new TimeConverter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd");
            }
            try{
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                JsonNode appname = objectNode.get("appname");
                JsonNode _appname = objectNode.get("_appname");
                if(_appname!=null && "metrics".equals(_appname.asText())) {
                    return out.iterator();
                } else if(appname!=null && "metrics".equals(appname.asText())){
                    String timestamp = objectNode.get("timeRcvd").asText();
                    out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
                } else {
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
                }
            } catch (Exception e){
                out.add(new Tuple2(BAD_DATA_LABLE, value));
            }
            return out.iterator();
        }
    }

    public static class TimeConverter implements Serializable {
        private SimpleDateFormat fromFormat;
        private SimpleDateFormat toFormat;
        public TimeConverter(String fromPattern, String toPattern){
            this.fromFormat = new SimpleDateFormat(fromPattern);
            this.toFormat = new SimpleDateFormat(toPattern);
        }

        public TimeConverter(String toPattern){
            this.toFormat = new SimpleDateFormat(toPattern);
        }

        public String convert(String timeStamp) throws Exception {
            return toFormat.format(fromFormat.parse(timeStamp));
        }

        public String convert(Timestamp timeStamp) throws Exception {
            return toFormat.format(timeStamp);
        }
    }
}

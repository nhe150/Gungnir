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

public class Functions {
    public static final class AppFilter implements FilterFunction<String> {
        private String tag;
        public AppFilter(String appName){
            this.tag = "\"_appname\":" + '"' + appName + '"';
        }

        public boolean call(String line){
            return line.contains(tag);
        }
    }

    public static final class PreProcess implements FlatMapFunction<String, Tuple2<String, String>> {
        private transient ObjectMapper objectMapper;
        private transient TimeConverter timeConverter;
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
                String message = objectNode.get("@message").asText();
                String timestamp = objectNode.get("@timestamp").asText();
                String[] jsonMessages = message.split(":", 2);
                if(jsonMessages.length == 2){
                    JsonNode metric = objectMapper.readTree(jsonMessages[1].trim());
                    objectNode.set("SM", metric);
                    objectNode.remove("@message");
                }
                out.add(new Tuple2(timeConverter.convert(timestamp), objectNode.toString()));
            } catch (Exception e){

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

    public static final class TestFilter implements FilterFunction<String> {
        public boolean call(String line){
            return line.contains("\"_appname\":\"conv\"") && line.contains("9e986e67-1d39-44e0-a2f0-4063b51eb708") && line.contains("\\\"contentSize\\\":1143") && line.contains("\\\"verb\\\":\\\"share\\\"");
        }
    }
}

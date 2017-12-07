import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.*;

import java.sql.Timestamp;
import java.util.*;

public class TableProcessor implements Serializable {
    private SparkSession spark;
    private String watermarkDelayThreshold = "2 day";
    private String aggregationPeriod = "daily";

    public TableProcessor(SparkSession spark){
        this.spark = spark;
        registerFunctions();
    }

    public String getAggregatePeriod(){
        return this.aggregationPeriod;
    }

    public void setAggregatePeriod(String period){
        if(period == null) return;
        switch (period){
            case "daily":
                setPeriod(period);
                setWatermarkDelayThreshold("2 day");
                break;
            case "weekly":
                setPeriod(period);
                setWatermarkDelayThreshold("8 days");
                break;
            case "monthly":
                setPeriod(period);
                setWatermarkDelayThreshold("32 days");
                break;
            default:
                System.out.println("Invalid input for aggregation period");
                System.exit(0);
        }
    }

    public StructType getSchema(String fileName) throws Exception {
        InputStream inputStream = getClass().getResourceAsStream(fileName);
        String jsonString = IOUtils.toString(inputStream);
        Dataset<String> tmpDataset = spark.createDataset(Arrays.asList(jsonString), Encoders.STRING());
        Dataset<Row> data = spark.read().json(tmpDataset);
        return data.schema();
    }

    public Dataset callQuality(Dataset raw){
        raw.createOrReplaceTempView("callQualityRaw");
        return spark.sql(sql.Queries.callQuality());
    }

    public Dataset callVolume(Dataset raw){
        raw.createOrReplaceTempView("callVolumeRaw");
        return spark.sql(sql.Queries.callVolume());
    }

    public Dataset callDuration(Dataset raw){
        raw.createOrReplaceTempView("callDurationRaw");
        return spark.sql(sql.Queries.callDuration());
    }

    public Dataset fileUsed(Dataset raw){
        raw.createOrReplaceTempView("fileUsedRaw");
        return spark.sql(sql.Queries.fileUsed());
    }

    public Dataset registeredEndpoint(Dataset raw){
        raw.createOrReplaceTempView("registeredEndpointRaw");
        return spark.sql(sql.Queries.registeredEndpoint());
    }

    public Dataset activeUser(Dataset raw){
        raw.createOrReplaceTempView("activeUsersRaw");
        spark.sql(sql.Queries.activeUsersFilter()).createOrReplaceTempView("activeUsersFiltered");
        return spark.sql(sql.Queries.activeUser());
    }

    public Dataset callQualityCount(Dataset callQuality){
        callQuality
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .selectExpr("aggregateStartDate(time_stamp) as time_stamp", "orgId", "call_id",
                        "CASE WHEN (audio_is_good=1) THEN 1 ELSE 0 END AS quality_is_good",
                        "CASE WHEN (audio_is_good=0) THEN 1 ELSE 0 END AS quality_is_bad")
                .dropDuplicates("time_stamp", "orgId", "call_id", "quality_is_good", "quality_is_bad")
                .createOrReplaceTempView("callQuality");
        return spark.sql(sql.Queries.callQualityCount());
    }

    public Dataset callVolumeCount(Dataset callVolume){
        callVolume
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .createOrReplaceTempView("callVolume");
        return spark.sql(sql.Queries.callVolumeCount());
    }

    public Dataset callDurationCount(Dataset callDuration){
        callDuration
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .createOrReplaceTempView("callDuration");
        return spark.sql(sql.Queries.callDurationCount());
    }

    public Dataset fileUsedCount(Dataset fileUsed){
        fileUsed
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .createOrReplaceTempView("fileUsed");
        return spark.sql(sql.Queries.fileUsedCount());
    }

    public Dataset registeredEndpointCount(Dataset registeredEndpoint){
        registeredEndpoint
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .selectExpr("aggregateStartDate(time_stamp) as time_stamp", "orgId", "model", "deviceId")
                .dropDuplicates("time_stamp", "orgId", "model", "deviceId")
                .createOrReplaceTempView("registeredEndpoint");
        return spark.sql(sql.Queries.registeredEndpointCount());
    }

    public List<Dataset> activeUserCounts(Dataset activeUser){
        List<Dataset> datasets = new ArrayList<>();
        Dataset activeUserWithWatermark = activeUser.withWatermark("time_stamp", watermarkDelayThreshold);
        activeUserWithWatermark.createOrReplaceTempView("activeUser");
        datasets.add(spark.sql(sql.Queries.isCreateSumByOrg()));
        datasets.add(activeUserCount(activeUserWithWatermark,"userId", "userCountByOrg"));
        datasets.add(activeUserCount(activeUserWithWatermark,"rtUser", "allUser"));
        datasets.add(activeUserCount(activeUserWithWatermark,"oneToOneUser", "oneToOneUser"));
        datasets.add(activeUserCount(activeUserWithWatermark,"groupUser", "spaceUser"));
        datasets.add(activeUserCount(activeUserWithWatermark,"teamUser", "teamUser"));
        datasets.add(activeUserCount(activeUserWithWatermark,"oneToOne", "oneToOneCount"));
        datasets.add(activeUserCount(activeUserWithWatermark,"group", "spaceCount"));
        datasets.add(activeUserCount(activeUserWithWatermark,"team", "teamCount"));
        return datasets;
    }

    public Dataset activeUserRollUp(Dataset activeUser){
        activeUser
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .selectExpr("endOfDay(time_stamp) as time_stamp", "orgId", "userId", "isMessage", "isCall")
                .createOrReplaceTempView("activeUser");
        return spark.sql(sql.Queries.activeUserRollUp());
    }

    public Dataset rtUser(Dataset activeUser){
        activeUser
                .selectExpr("endOfDay(time_stamp) as time_stamp", "orgId", "userId", "oneToOne", "group")
                .createOrReplaceTempView("activeUser");
        return spark.sql(sql.Queries.rtUser());
    }

    public Dataset activeUserTopCount(Dataset activeUser){
        activeUser
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .createOrReplaceTempView("activeUser");
        return spark.sql(sql.Queries.topUser());
    }

    public Dataset topPoorQuality(Dataset callQuality){
        callQuality
                .withWatermark("time_stamp", watermarkDelayThreshold)
                .selectExpr("aggregateStartDate(time_stamp) as time_stamp", "orgId", "userId",
                        "CASE WHEN (audio_is_good=0 AND video_is_good=0) THEN 1 ELSE 0 END AS quality_is_bad")
                .createOrReplaceTempView("callQuality");
        return spark.sql(sql.Queries.topPoorQuality());
    }

    private Dataset activeUserCount(Dataset activeUser, String aggregateColumn, String resultColumn){
        activeUser
                .selectExpr("aggregateStartDate(time_stamp) as time_stamp", "orgId", aggregateColumn)
                .dropDuplicates("time_stamp", "orgId", aggregateColumn).createOrReplaceTempView(resultColumn);
        return spark.sql(sql.Queries.activeUserCountQuery(aggregateColumn, resultColumn));
    }

    private void setPeriod(String period){
        this.aggregationPeriod = period;
        spark.udf().register("periodTag", new PeriodTag(period), DataTypes.StringType);
        spark.udf().register("aggregateStartDate", new AggregateStartDate(period), DataTypes.TimestampType);
    }

    private void setWatermarkDelayThreshold(String threshold){
        this.watermarkDelayThreshold = threshold;
    }

    private void registerFunctions(){
        spark.udf().register("validOrg", new ValidOrgLookup("/officialOrgList.csv"), DataTypes.StringType);
        spark.udf().register("convertTime", new TimeConverter("yyyy-MM-dd"), DataTypes.StringType);
        spark.udf().register("calcAvgFromHistMin", new calcAvgFromHistMin(), DataTypes.FloatType);
        spark.udf().register("periodTag", new PeriodTag(aggregationPeriod), DataTypes.StringType);
        spark.udf().register("aggregateStartDate", new AggregateStartDate(aggregationPeriod), DataTypes.TimestampType);
        spark.udf().register("endOfDay", new EndOfDay(), DataTypes.TimestampType);
        spark.udf().register("uuid", new Uuid(), DataTypes.StringType);
        spark.udf().register("shortUuid", new shortUuid(), DataTypes.StringType);
    }

    private class calcAvgFromHistMin implements UDF1<WrappedArray<GenericRowWithSchema>, Float> {
        public Float call(WrappedArray<GenericRowWithSchema> array) throws Exception {
            if(array == null || array.length() == 0) return 0 * 1.0f;
            List<GenericRowWithSchema> list = JavaConversions.seqAsJavaList(array);
            Long durationSum = list.stream()
                    .map(x -> x.getLong(x.fieldIndex("duration")))
                    .reduce(0L, (x, y) -> x + y);
            Long totalSum = list.stream()
                    .map(x -> x.getLong(x.fieldIndex("duration")) * (x.getLong(x.fieldIndex("min")) + 1))
                    .reduce(0L, (x, y) -> x + y);
            if(durationSum == 0) return 0 * 1.0f;
            return totalSum * 1.0f / durationSum;
        }
    }

    private class PeriodTag implements UDF1<String, String> {
        private String period;
        public PeriodTag(String period){
            this.period = period;
        }
        public String call(String dummy) throws Exception {
            return period;
        }
    }

    private class Uuid implements UDF1<String, String> {
        public String call(String dummy) throws Exception {
            return UUID.randomUUID().toString();
        }
    }

    private class shortUuid implements UDF1<String, String> {
        public String call(String dummy) throws Exception {
            return UUID.nameUUIDFromBytes(dummy.getBytes()).toString();
        }
    }

    private class EndOfDay implements UDF1<Timestamp, Timestamp> {
        public Timestamp call(Timestamp timeStamp) throws Exception {
            DateTime dateTime = new DateTime(timeStamp);
            return new Timestamp(dateTime.plusDays(1).withTimeAtStartOfDay().minusSeconds(1).getMillis());
        }
    }

    private class AggregateStartDate implements UDF1<Timestamp, Timestamp> {
        private String period;
        public AggregateStartDate(String period){
            this.period = period;
        }
        public Timestamp call(Timestamp timeStamp) throws Exception {
            DateTime dateTime = new DateTime(timeStamp);
            switch (period){
                case "weekly":
                    return new Timestamp(dateTime.withDayOfWeek(1).withMillisOfDay(0).getMillis());
                case "monthly":
                    return new Timestamp(dateTime.withDayOfMonth(1).withMillisOfDay(0).getMillis());
                default:
                    return new Timestamp(dateTime.withMillisOfDay(0).getMillis());
            }
        }
    }

    private class TimeConverter implements UDF1<Timestamp, String> {
        private Functions.TimeConverter timeConverter;
        public TimeConverter(String toPattern){
            this.timeConverter = new Functions.TimeConverter(toPattern);
        }

        public String call(Timestamp timeStamp) throws Exception {
            return timeConverter.convert(timeStamp);
        }
    }

    private class ValidOrgLookup implements UDF1<String, String> {
        private Map<String, String> orgExcludeMap;

        public ValidOrgLookup(String fileName){
            this.orgExcludeMap = parseUsingOpenCSV(fileName);
        }

        public Map<String, String> parseUsingOpenCSV(String fileName) {
            Map<String, String> orgExcludeMap = new HashMap();
            InputStream inputStream = getClass().getResourceAsStream(fileName);
            List<String[]> records = null;

            CSVReader reader =
                    new CSVReader(
                            new InputStreamReader(inputStream),
                            CSVParser.DEFAULT_SEPARATOR,
                            CSVParser.DEFAULT_QUOTE_CHARACTER, 1);

            try {
                records = reader.readAll();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Iterator<String[]> iterator = records.iterator();

            while (iterator.hasNext()) {
                String[] record = iterator.next();
                orgExcludeMap.put(record[0], record[7]);
            }

            return orgExcludeMap;
        }

        public String call(String orgId) throws Exception {
            return orgExcludeMap.get(orgId);
        }
    }

}

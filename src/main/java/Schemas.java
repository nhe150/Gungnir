import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class Schemas {
    public static final Map<String, StructType> schema;

    public static final StructType callQualitySchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("duration", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("mediaType", DataTypes.StringType, true, Metadata.empty()),
            new StructField("call_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("audio_jitter", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("video_jitter", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("audio_rtt", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("video_rtt", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("audio_packetloss", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("video_packetloss", DataTypes.FloatType, true, Metadata.empty()),
            new StructField("uaVersion", DataTypes.StringType, true, Metadata.empty()),
            new StructField("uaType", DataTypes.StringType, true, Metadata.empty()),
            new StructField("source", DataTypes.StringType, true, Metadata.empty()),
            new StructField("confId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("meetingId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("audio_is_good", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("video_is_good", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType callVolumeSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("uaVersion", DataTypes.StringType, true, Metadata.empty()),
            new StructField("uaType", DataTypes.StringType, true, Metadata.empty()),
            new StructField("callFailure", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("source", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType callDurationSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("uaVersion", DataTypes.StringType, true, Metadata.empty()),
            new StructField("uaType", DataTypes.StringType, true, Metadata.empty()),
            new StructField("legDuration", DataTypes.LongType, true, Metadata.empty()),
            new StructField("source", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType fileUsedSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("isFile", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("contentSize", DataTypes.LongType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType activeUserSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("isMessage", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("isCall", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("isCreate", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("rtUser", DataTypes.StringType, true, Metadata.empty()),
            new StructField("oneToOneUser", DataTypes.StringType, true, Metadata.empty()),
            new StructField("groupUser", DataTypes.StringType, true, Metadata.empty()),
            new StructField("teamUser", DataTypes.StringType, true, Metadata.empty()),
            new StructField("oneToOne", DataTypes.StringType, true, Metadata.empty()),
            new StructField("group", DataTypes.StringType, true, Metadata.empty()),
            new StructField("team", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType registeredEndpointSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("deviceId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("model", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}

    );

    public static final StructType callQualityCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("number_of_good_calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("number_of_bad_calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType callDurationCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("number_of_minutes", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType callVolumeCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("number_of_failed_calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("number_of_successful_calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType fileUsedCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("files", DataTypes.LongType, true, Metadata.empty()),
            new StructField("fileSize", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType registeredEndpointCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("model", DataTypes.StringType, true, Metadata.empty()),
            new StructField("registeredEndpointCount", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType activeUserCountSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userCountByOrg", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType topUserSchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("messages", DataTypes.LongType, true, Metadata.empty()),
            new StructField("calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType topPoorQualitySchema = new StructType(new StructField[]{
            new StructField("eventKey", DataTypes.StringType, true, Metadata.empty()),
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("number_of_bad_calls", DataTypes.LongType, true, Metadata.empty()),
            new StructField("period", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType activeUserRollUpSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("isMessage", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("isCall", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    public static final StructType rtUserSchema = new StructType(new StructField[]{
            new StructField("time_stamp", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("pdate", DataTypes.StringType, true, Metadata.empty()),
            new StructField("dataid", DataTypes.StringType, true, Metadata.empty()),
            new StructField("orgId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
            new StructField("oneToOne", DataTypes.StringType, true, Metadata.empty()),
            new StructField("group", DataTypes.StringType, true, Metadata.empty()),
            new StructField("relation_name", DataTypes.StringType, true, Metadata.empty())}
    );

    static
    {
        schema = new HashMap<>();
        schema.put("callQuality", callQualitySchema);
        schema.put("callVolume", callVolumeSchema);
        schema.put("callDuration", callDurationSchema);
        schema.put("fileUsed", fileUsedSchema);
        schema.put("activeUser", activeUserSchema);
        schema.put("registeredEndpoint", registeredEndpointSchema);
    }

}

package com.cisco.gungnir.pipelines

import org.apache.spark.sql.types._

object Schema {
  def schema = StructType(
      StructField("measurement", StringType, false) ::
      StructField("mq_metric_type", StringType, false) ::
      StructField("crid_media_type", StringType, false) ::
      StructField("correlation_id", StringType, false) ::
      StructField("time", StringType,false)::
      StructField("device_type", StringType, false) ::
      StructField("session_type", StringType, false) ::
      StructField("network_type", StringType, false) ::
      StructField("media_agent_type", StringType, false) ::
      StructField("crid_media_score", StringType, false) ::
      StructField("crid_media_audio_score", StringType, false) ::
      StructField("crid_media_video_score", StringType, false) ::
      StructField("crid_media_share_score", StringType, false) ::
      StructField("crid_media_reason", StringType, false) ::
      StructField("server_group", StringType, true) ::
      StructField("server_org", StringType, true) ::
      StructField("is_cascade", StringType, false) ::
      StructField("remote_server_group", StringType, true) ::
      StructField("remote_server_org", StringType, true) ::
      StructField("client_region", StringType, true) ::
      StructField("count", StringType, true) ::
      StructField("locus_session_id", StringType, false) ::
      StructField("org_id", StringType, false) ::
      StructField("user_id", StringType, true) ::
      StructField("tracking_id", StringType, false) ::
      StructField("locus_id", StringType, false) ::
      StructField("locus_start_time", StringType, true) ::
      StructField("client_media_engine_software_version", StringType, false) ::
      StructField("device_version", StringType, false) ::
      StructField("server_alias", StringType, true) ::
      StructField("remote_server_alias", StringType, true) ::
      StructField("labels", StringType, false) ::
      StructField("ip_reflexive_addr", StringType, false) ::
      StructField("start_time", StringType,false)::


      StructField("crid_media_audio_metrics", StructType(
        StructField("rx_media_hop_lost",ArrayType(LongType,false),false)::
          StructField("rx_rtp_pkts",ArrayType(LongType,false),false)::
          StructField("tx_rtt",ArrayType(LongType,false),false)::
          StructField("rx_media_e2e_lost_percent",ArrayType(LongType,false),false)::
          StructField("rx_media_session_jitter",ArrayType(LongType,false),false)::
          Nil
      ),false) ::

      StructField("crid_media_video_metrics", StructType(
        StructField("rx_media_hop_lost",ArrayType(LongType,false),false)::
        StructField("rx_rtp_pkts",ArrayType(LongType,false),false)::
          StructField("tx_rtt",ArrayType(LongType,false),false)::
          StructField("rx_media_e2e_lost_percent",ArrayType(LongType,false),false)::
          StructField("rx_media_session_jitter",ArrayType(LongType,false),false)::
          Nil
      ),false) ::

      StructField("crid_media_share_metrics", StructType(
        StructField("rx_media_hop_lost",ArrayType(LongType,false),false)::
          StructField("rx_rtp_pkts",ArrayType(LongType,false),false)::
          StructField("tx_rtt",ArrayType(LongType,false),false)::
          StructField("rx_media_e2e_lost_percent",ArrayType(LongType,false),false)::
          StructField("rx_media_session_jitter",ArrayType(LongType,false),false)::
          Nil
      ),false) ::

      StructField("tx_rtp_pkts", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false) ::

      StructField("tx_avail_bitrate", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      StructField("tx_rtp_bitrate", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      StructField("tx_queue_delay", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      StructField("tx_rtt", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::


      StructField("rx_rtp_pkts", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::


      StructField("rx_media_hop_lost", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::


      StructField("rx_rtp_bitrate", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      StructField("rx_media_e2e_lost_percent", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      StructField("rx_media_session_jitter", StructType(
        StructField("audio",ArrayType(LongType,false),false)::
          StructField("video",ArrayType(LongType,false),false)::
          StructField("share",ArrayType(LongType,false),false):: Nil
      ),false)::

      Nil
  )
}

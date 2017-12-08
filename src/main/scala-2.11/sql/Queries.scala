package sql

object Queries {
  def callQuality =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       uuid(orgId) as dataid,
      |       orgId,
      |       userId,
      |       duration,
      |       mediaType,
      |       call_id,
      |       audio_jitter,
      |       video_jitter,
      |       audio_rtt,
      |       video_rtt,
      |       audio_packetloss,
      |       video_packetloss,
      |       uaVersion,
      |       uaType,
      |       source,
      |       confId,
      |       meetingId,
      |       CASE
      |           WHEN (mediaType='callEnd_audio'
      |                 AND audio_jitter<=150
      |                 AND audio_rtt<=400
      |                 AND audio_packetloss<=0.05) THEN 1
      |           ELSE 0
      |       END AS audio_is_good,
      |       CASE
      |           WHEN (mediaType='callEnd_video') THEN 1
      |           ELSE 0
      |       END AS video_is_good,
      |       'callQuality' AS relation_name
      |FROM
      |  (SELECT to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp,
      |          SM.value.clientCallDuration AS duration,
      |          coalesce(`@fields`.orgId, 'unknown') AS orgId,
      |          SM.key AS mediaType,
      |          coalesce(`@fields`.userId, "unknown") AS userId,
      |          CONCAT(SM.value.locusId , '^', SM.value.locusTimestamp) AS call_id,
      |          SM.sessionId as confId,
      |          SM.value.correlationId as meetingId,
      |
      |          CASE
      |              WHEN (SM.key='callEnd_audio') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.jitter)
      |          END AS audio_jitter,
      |          CASE
      |              WHEN (SM.key='callEnd_video') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.jitter)
      |          END AS video_jitter,
      |          CASE
      |              WHEN (SM.key='callEnd_audio') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.rtt)
      |          END AS audio_rtt,
      |          CASE
      |              WHEN (SM.key='callEnd_video') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.rtt)
      |          END AS video_rtt,
      |          CASE
      |              WHEN (SM.key='callEnd_audio') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.lossRatio)
      |          END AS audio_packetloss,
      |          CASE
      |              WHEN (SM.key='callEnd_video') THEN calcAvgFromHistMin(SM.value.mediaStatistics.stats.lossRatio)
      |          END AS video_packetloss,
      |          `@fields`.uaType AS uaType,
      |          `@fields`.uaVersion AS uaVersion,
      |          'Spark' AS SOURCE
      |   FROM callQualityRaw
      |   WHERE _appname='metrics'
      |     AND SM.value.callWasJoined=true
      |     AND SM.value.clientCallDuration>10000
      |     AND (SM.key='callEnd_audio'
      |          OR SM.key='callEnd_video'))
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def callQualityGoodCount =
    """
      |SELECT CONCAT(time_stamp, '^', orgId, '^', 'callQuality', '^', periodTag(orgId)) AS eventKey,
      |       time_stamp,
      |       orgId,
      |       sum(quality_is_good) AS number_of_good_calls,
      |       periodTag(orgId) AS period,
      |       'callQuality' AS relation_name
      |FROM callQuality
      |GROUP BY time_stamp,
      |         orgId

    """.stripMargin

  def callQualityBadCount =
    """
      |SELECT CONCAT(time_stamp, '^', orgId, '^', 'callQuality', '^', periodTag(orgId)) AS eventKey,
      |       time_stamp,
      |       orgId,
      |       sum(quality_is_bad) AS number_of_bad_calls,
      |       periodTag(orgId) AS period,
      |       'callQuality' AS relation_name
      |FROM callQuality
      |GROUP BY time_stamp,
      |         orgId

    """.stripMargin

  def callVolume =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       uuid(orgId) as dataid,
      |       orgId,
      |       userId,
      |       uaVersion,
      |       uaType,
      |       callFailure,
      |       'Spark' AS SOURCE,
      |       call_id,
      |       'callVolume' AS relation_name
      |FROM
      |  (SELECT coalesce(`@fields`.orgId, 'unknown') AS orgId,
      |          to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp,
      |          `@fields`.uaVersion AS uaVersion,
      |          `@fields`.uaType AS uaType,
      |          coalesce(`@fields`.userId, 'unknown') AS userId,
      |          CONCAT(SM.value.locusId , '^', SM.value.locusTimestamp) AS call_id,
      |          CASE
      |              WHEN (SM.key='Failures'
      |                    AND SM.value.error_code<3000
      |                    AND SM.value.system_status_code<>12007) THEN 1
      |              ELSE 0
      |          END AS callFailure
      |   FROM callVolumeRaw
      |   WHERE _appname='metrics'
      |     AND (`@fields`.uaType='sparkmac'
      |          OR `@fields`.uaType='sparkwindows')
      |     AND ((SM.key='callEnd'
      |           AND SM.value.callWasJoined=true)
      |          OR (SM.key='Failures'
      |              AND SM.value.error_name='call_join'
      |              AND SM.value.error_code<>2003))
      |     AND (NOT (SM.value.locusTimestamp=''
      |               OR SM.value.locusTimestamp='1970-01-01T00:00:00.000Z'
      |               OR SM.value.locusId=''
      |               OR SM.value.locusTimestamp IS NULL
      |               OR SM.value.locusId IS NULL)))
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def callVolumeCount =
    """
      |SELECT CONCAT(aggregateStartDate(time_stamp), '^', orgId, '^', 'callVolume', '^', periodTag(orgId)) AS eventKey,
      |       aggregateStartDate(time_stamp) AS time_stamp,
      |       orgId,
      |       sum(callFailure) AS number_of_failed_calls,
      |       sum(1-callFailure) AS number_of_successful_calls,
      |       periodTag(orgId) AS period,
      |       'callVolume' AS relation_name
      |FROM callVolume
      |GROUP BY orgId,
      |         aggregateStartDate(time_stamp)
    """.stripMargin

  def callDuration =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       uuid(orgId) as dataid,
      |       orgId,
      |       userId,
      |       uaVersion,
      |       uaType,
      |       legDuration,
      |       'Spark' AS SOURCE,
      |       call_id,
      |       'callDuration' AS relation_name
      |FROM
      |  (SELECT coalesce(SM.orgId, SM.participant.orgId, 'unknown') AS orgId,
      |          to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp,
      |          SM.legDuration AS legDuration,
      |          coalesce(`@fields`.USER_ID, SM.actor.id, SM.participant.userId, SM.userId, SM.uid, SM.onBoardedUser, 'unknown') AS userId,
      |          `@fields`.uaVersion AS uaVersion,
      |          CONCAT(`@fields`.LOCUS_ID , '^', `@fields`.locusStartTime) AS call_id,
      |          SM.uaType AS uaType
      |   FROM callDurationRaw
      |   WHERE _appname='locus'
      |     AND SM.metricType='CALL_LEG'
      |     AND (SM.uaType='sparkwindows'
      |          OR SM.uaType='sparkmac')
      |     AND SM.callType='TERMINATED')
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def callDurationCount =
    """
      |SELECT CONCAT(aggregateStartDate(time_stamp), '^', orgId, '^', 'callDuration', '^', periodTag(orgId)) AS eventKey,
      |       aggregateStartDate(time_stamp) AS time_stamp,
      |       orgId,
      |       CAST(round(sum(legDuration)/60) AS BIGINT) AS number_of_minutes,
      |       periodTag(orgId) AS period,
      |       'callDuration' AS relation_name
      |FROM callDuration
      |GROUP BY orgId,
      |         aggregateStartDate(time_stamp)
    """.stripMargin

  def fileUsed =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       CONCAT('fileUsed', '^', time_stamp, '^', orgId, '^', userId, '^', contentSize) AS dataid,
      |       orgId,
      |       userId,
      |       isFile,
      |       contentSize,
      |       'fileUsed' AS relation_name
      |FROM
      |  (SELECT to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp,
      |          coalesce(orgId, SM.actor.orgId, SM.participant.orgId, SM.orgId, 'unknown') AS orgId,
      |          coalesce(userId, `@fields`.USER_ID, SM.actor.id, SM.participant.userId, SM.userId, SM.uid, SM.onBoardedUser, 'unknown') AS userId,
      |          CASE
      |              WHEN SM.object.objectType='content' THEN 1
      |              ELSE 0
      |          END AS isFile,
      |          SM.object.contentSize AS contentSize
      |   FROM fileUsedRaw
      |   WHERE _appname='conv'
      |     AND SM.verb='share')
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def fileUsedCount =
    """
      |SELECT CONCAT(aggregateStartDate(time_stamp), '^', orgId, '^', 'fileUsed', '^', periodTag(orgId)) AS eventKey,
      |       aggregateStartDate(time_stamp) AS time_stamp,
      |       orgId,
      |       SUM(isFile) AS files,
      |       SUM(contentSize) AS fileSize,
      |       periodTag(orgId) AS period,
      |       'fileUsed' AS relation_name
      |FROM fileUsed
      |GROUP BY orgId,
      |         aggregateStartDate(time_stamp)
    """.stripMargin

  def activeUsersFilter =
    """
      |SELECT CASE
      |           WHEN (SM.verb='create'
      |                 AND (SM.object.objectType='team'
      |                      OR SM.object.objectType='conversation')) THEN 1
      |           ELSE 0
      |       END AS isCreate,
      |       CASE
      |           WHEN (SM.convOneOnOne=true) THEN 1
      |           ELSE 0
      |       END AS isOneToOneRoom,
      |       CASE
      |           WHEN (`@fields`.teamId IS NULL
      |                 AND SM.convOneOnOne=false) THEN 1
      |           ELSE 0
      |       END AS isGroupRoom,
      |       CASE
      |           WHEN `@fields`.teamId IS NOT NULL THEN 1
      |           ELSE 0
      |       END AS isTeamRoom,
      |       CASE
      |           WHEN (`@fields`.teamId IS NULL
      |                 AND SM.convOneOnOne=false) THEN SM.target.id
      |       END AS isGroupRoomId,
      |       CASE
      |           WHEN (SM.convOneOnOne=true) THEN SM.target.id
      |       END AS isOneToOneRoomId,
      |       CASE
      |           WHEN `@fields`.teamId IS NOT NULL THEN SM.target.id
      |       END AS isTeamRoomId,
      |       CASE
      |           WHEN SM.verb='post'
      |                AND SM.object.objectType='comment' THEN 1
      |           ELSE 0
      |       END AS isMessage,
      |       CASE
      |           WHEN SM.metricType='PARTICIPANT'
      |                AND SM.callType='TERMINATED' THEN 1
      |           ELSE 0
      |       END AS isCall,
      |       coalesce(orgId, SM.actor.orgId, SM.participant.orgId, SM.orgId, 'unknown') AS orgId,
      |       coalesce(userId, `@fields`.USER_ID, SM.actor.id, SM.participant.userId, SM.userId, SM.uid, SM.onBoardedUser, 'unknown') AS userId,
      |       to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp
      |FROM activeUsersRaw
      |WHERE _appname='conv'
      |  AND SM.uaType<>'Messenger'
      |  AND SM.uaType<>'Hydra'
      |  AND SM.uaType<>'UCConnector'
      |  AND ((SM.verb='acknowledge'
      |        AND SM.object.objectType='activity')
      |       OR (SM.verb='post'
      |           AND SM.object.objectType='comment')
      |       OR (SM.verb='share'
      |           AND SM.object.objectType='content')
      |       OR (SM.verb='create'
      |           AND (SM.object.objectType='conversation'
      |                OR SM.object.objectType='team'))
      |       OR (SM.verb='add'
      |           AND SM.object.objectType='person'
      |           AND (SM.target.objectType='conversation'
      |                OR SM.object.objectType='team')))
      |  OR (_appname='locus'
      |      AND (SM.metricType='PARTICIPANT'
      |           AND SM.callType='TERMINATED'
      |           AND SM.squared=true
      |           AND SM.locusType<>'JABBER'))
    """.stripMargin

  def activeUser =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       uuid(orgId) as dataid,
      |       orgId,
      |       userId,
      |       isMessage,
      |       isCall,
      |       isCreate,
      |       CASE
      |           WHEN (isOneToOneRoom=1
      |                 OR isGroupRoom=1
      |                 OR isTeamRoom=1) THEN userId
      |       END AS rtUser,
      |       CASE
      |           WHEN (isOneToOneRoom=1) THEN userId
      |       END AS oneToOneUser,
      |       CASE
      |           WHEN (isGroupRoom=1) THEN userId
      |       END AS groupUser,
      |       CASE
      |           WHEN (isTeamRoom=1) THEN userId
      |       END AS teamUser,
      |       CASE
      |           WHEN (isOneToOneRoom=1) THEN isOneToOneRoomId
      |       END AS oneToOne,
      |       CASE
      |           WHEN (isGroupRoom=1) THEN isGroupRoomId
      |       END AS group,
      |       CASE
      |           WHEN (isTeamRoom=1) THEN isTeamRoomId
      |       END AS team,
      |       'activeUser' AS relation_name
      |FROM activeUsersFiltered
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def registeredEndpoint =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       uuid(orgId) as dataid,
      |       orgId,
      |       userId,
      |       deviceId,
      |       model,
      |       'registeredEndpoint' AS relation_name
      |FROM
      |  (SELECT to_timestamp(`@timestamp`, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS time_stamp,
      |          coalesce(`@fields`.orgId, 'unknown') AS orgId,
      |          SM.deviceIdentifier AS deviceId,
      |          coalesce(
      |             CASE
      |                 WHEN (SM.value.model='Room 70D') THEN 'Undefined'
      |                 WHEN (SM.value.model='SparkBoard 55') THEN 'SPARK-BOARD55'
      |                 ELSE SM.value.model
      |             END, 'unknown') AS model,
      |          'NA' AS userId
      |   FROM registeredEndpointRaw
      |   WHERE _appname='metrics'
      |     AND (`@fields`.uaType='ce'
      |          OR `@fields`.uaType='SparkBoard')
      |     AND (SM.key='sysinfo_darling'
      |          OR SM.key='sysinfo'))
      |WHERE orgId<>'unknown'
      |  AND validOrg(orgId)<>'1'
    """.stripMargin

  def registeredEndpointCount =
    """
      |SELECT CONCAT(time_stamp, '^', orgId, '^', model, '^', 'registeredEndpoint', '^', periodTag(orgId)) AS eventKey,
      |       time_stamp,
      |       orgId,
      |       model,
      |       COUNT(deviceId) AS registeredEndpointCount,
      |       periodTag(orgId) AS period,
      |       'registeredEndpoint' AS relation_name
      |FROM registeredEndpoint
      |GROUP BY orgId,
      |         time_stamp,
      |         model
    """.stripMargin

  def isCreateSumByOrg =
    """
      |SELECT CONCAT(aggregateStartDate(time_stamp), '^', orgId, '^', 'activeUser', '^', periodTag(orgId)) AS eventKey,
      |       aggregateStartDate(time_stamp) AS time_stamp,
      |       orgId,
      |       SUM(isCreate) AS createAllNew,
      |       periodTag(orgId) AS period,
      |       'activeUser' AS relation_name
      |FROM activeUser
      |GROUP BY orgId,
      |         aggregateStartDate(time_stamp)
    """.stripMargin

  def activeUserCountQuery(aggregateColumn: String, resultColumn: String) =
    s"""
      |SELECT CONCAT(time_stamp, '^', orgId, '^', 'activeUser', '^', periodTag(orgId)) AS eventKey,
      |       time_stamp,
      |       orgId,
      |       COUNT(${aggregateColumn}) AS ${resultColumn},
      |       periodTag(orgId) AS period,
      |       'activeUser' AS relation_name
      |FROM ${resultColumn}
      |GROUP BY orgId,
      |         time_stamp
    """.stripMargin

  def topUser =
    """
      |SELECT CONCAT(aggregateStartDate(time_stamp), '^', orgId, '^', userId, '^', 'topUser', '^', periodTag(orgId)) AS eventKey,
      |       aggregateStartDate(time_stamp) as time_stamp,
      |       orgId,
      |       userId,
      |       sum(isMessage) AS messages,
      |       sum(isCall) AS calls,
      |       periodTag(orgId) AS period,
      |       'topUser' AS relation_name
      |FROM activeUser
      |GROUP BY orgId,
      |         userId,
      |         aggregateStartDate(time_stamp)
      |ORDER BY messages + calls DESC
      |LIMIT 25
    """.stripMargin

  def topPoorQuality =
    """
      |SELECT CONCAT(time_stamp, '^', orgId, '^', userId, '^', 'topPoorQuality', '^', periodTag(orgId)) AS eventKey,
      |       time_stamp,
      |       orgId,
      |       userId,
      |       sum(quality_is_bad) AS number_of_bad_calls,
      |       periodTag(orgId) AS period,
      |       'topPoorQuality' AS relation_name
      |FROM callQuality
      |GROUP BY orgId,
      |         userId,
      |         time_stamp
      |ORDER BY number_of_bad_calls DESC
      |LIMIT 25
    """.stripMargin


  def activeUserRollUp =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       CONCAT(convertTime(time_stamp), '^', orgId, '^', userId) as dataid,
      |       orgId,
      |       userId,
      |       sum(isMessage) AS isMessage,
      |       sum(isCall) AS isCall,
      |       'activeUserRollUp' AS relation_name
      |FROM activeUser
      |GROUP BY orgId,
      |         userId,
      |         time_stamp
    """.stripMargin

  def rtUser =
    """
      |SELECT time_stamp,
      |       convertTime(time_stamp) as pdate,
      |       CONCAT(convertTime(time_stamp), '^', orgId, '^rtUser^', CASE WHEN oneToOne IS NOT NULL THEN oneToOne ELSE 0 END, '^', CASE WHEN group IS NOT NULL THEN group ELSE 0 END) as dataid,
      |       orgId,
      |       userId,
      |       oneToOne,
      |       group,
      |       'rtUser' AS relation_name
      |FROM activeUser
    """.stripMargin

}

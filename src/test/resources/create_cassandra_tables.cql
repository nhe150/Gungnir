create table license (
pdate text,
relation_name text,
time_stamp timestamp,
dataid text,
associated_key text,
orgid text,
userId text,
adminId text,
onboardMethod text,
licensesAssign text,
licenses text,
status text,
errormessage text,
primary key ( (orgid, relation_name), pdate, dataid)
);

CREATE TABLE spark_agg (
eventKey text,
time_stamp timestamp,
orgId text,
userId text,
model text,
number_of_total_calls bigint,
number_of_bad_calls bigint,
number_of_failed_calls bigint,
number_of_successful_calls bigint,
number_of_minutes bigint,
files bigint,
fileSize bigint,
registeredEndpointCount bigint,
createAllNew bigint,
userCountByOrg bigint,
allUser bigint,
oneToOneUser bigint,
spaceUser bigint,
teamUser bigint,
oneToOneCount bigint,
spaceCount bigint,
teamCount bigint,
messages bigint,
calls bigint,
period text,
associated_key text,
relation_name text,
ep1 text,
devicetype text,
uatype text,
primary key(eventKey, time_stamp)

);

CREATE TABLE spark_data (
pdate text,
relation_name text,
time_stamp timestamp,
dataid text,
orgId text,
duration float,
mediaType text,
call_id text,
audio_jitter float,
video_jitter float,
audio_rtt float,
video_rtt float,
audio_packetloss float,
video_packetloss float,
uaVersion text,
uaType text,
source text,
audio_is_good int,
video_is_good int,
userId text,
callFailure int,
legDuration bigint,
isFile int,
contentSize bigint,
isMessage int,
isCall int,
isCreate int,
RTUser text,
OneToOneUser text,
GroupUser text,
TeamUser text,
OneToOne text,
Group text,
Team text,
deviceId text,
model text,
confid text,
meetingId text,
callLeg text,
associated_key text,
devicetype text,
appname text,
locustype text,
activity text,
ua_category text,
primary key((pdate, relation_name), time_stamp, dataid)
);

insert into license(pdate, relation_name, orgid, userid, status, dataid) values('2019-07-23', 'license', 'orgA', 'userA', 'SUCCESS', 't001');
insert into license(pdate, relation_name, orgid, userid, status, dataid) values('2019-07-15', 'license', 'orgA', 'userA', 'SUCCESS', 't002');
insert into license(pdate, relation_name, orgid, userid, status, dataid) values('2019-07-15', 'license', 'orgA', 'userB', 'SUCCESS', 't003');

insert into license(pdate, relation_name, orgid, userid, status, dataid) values('2019-07-08', 'license', 'orgA', 'userA', 'SUCCESS', 't004');

//INSERT INTO spark_data(pdate, relation_name, time_stamp, dataid) values('test','test', '2017-11-08T22:30:47.000Z', 'id');

//INSERT INTO spark_data(pdate, relation_name, time_stamp, dataid) values('test2','test2', '2017-11-08T22:30:47.000Z', 'id2');
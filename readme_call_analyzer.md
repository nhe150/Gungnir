## Call Analyzer Anomaly Detection
This project read call analyzer data from ELK, generate history average model in ELK, 
and compare each org's yesterday average with history average to generate alert messages, 
send them to ELK with the format defined in: https://wiki.cisco.com/display/NEWPORTAL/WAP+pipeline+metrics+standarization+into+ELK+system

### Class
* Main class used in CMP: src/main/java/com/cisco/gungnir/pipelines/QoSMonitor.java
* Data processing serializable:  src/main/java/com/cisco/gungnir/job/QoSDataMonitor.java 

### Default Parameters

* numOrgs: 50 (integer):  how many orgs to monitor
* threshold: 0.33 (float) : threshold to generate alerts
* initialize: false (boolean): if or not update history data model
* Istest: false (boolean): if or not use test ELK index to do unit test
* Duration: 30 (int): how many business days used to generate history model
* Index BTS: https://clpsj-bts-call.webex.com/call 
* Index PROD: https://clpsj-call.webex.com/call/ 

#### ELK default indexs
* Data source index: "call_analyzer" 
* History data model index: "call_analyzer_anomalydetection"
* Alert index: "call_analyzer_alert"

### Unit Test Parameters

* numOrgs: 50 
* threshold: 0.8
* initialize: true 
* Istest: true 
* Duration: 30 
* currentDate = "2019-01-17"


#### ELK unit test indexs
* Data source index: "call_analyzer_unit_test" 
* History data model index: "call_analyzer_model_test"
* Alert index: "call_analyzer_alert_test"


#### Unit Test Data Info
There are total 100 documents in ELK unit test index, with both old and new schema mixed. 
The definition of lower and upper alert boundary is defined below:  

     * Lower alert bound: threshold => 0.8
     * Upper alert bound: 2/threshold => 2.5

The test documents include data from 4 orgs (A,B,C,D for instance), on 1/14, 1/15, 1/16 sperately. 
Those data will generate two alerts where one is too many calls and the other is too less calls:

     Org  1/14 1/15 1/16 AVG   Yesterday  %            Alert  
     A    0    14   2    8     2          0.25 < 0.8   true  
     B    2    2    70   24.6  70         2.8  > 2.5   true  
     C    0    2    4    3     4          1.3          false  
     D    0    2    2    2     2          1            false  
     

### CMP Job Parameter
Please follow below CMP parameters to submit jobs, otherwise there might be random failures due to Spark memory limitation:

- Executor: 16
-	vCore per Spark executor: 4
-	memory per executor: 32G
- Driver memory: 32G

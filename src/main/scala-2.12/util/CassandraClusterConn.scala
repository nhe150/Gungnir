package util

import com.datastax.driver.core
import com.datastax.driver.core.Cluster
import util.AppDatabase.log

class CassandraClusterConn(ips: Array[String], keyspace: String, user: String, pass: String) {
  var cluster: Cluster = null
  @volatile var session: core.Session = if (keyspace != null) {
    cluster = getCluster()
    val startTime = java.lang.System.currentTimeMillis();
    val se = cluster.connect(this.keyspace)
    val endTime = java.lang.System.currentTimeMillis();
    val timeInterval = endTime - startTime
    log.info("call cluster.connect(keyspace) is " + timeInterval + " milliseconds (note, get cluster timing is NOT included here, just connecting to cluster to get session)")
    se
  } else {
    if (user!= null) getCluster().connect() else null
  }

  def close() = {
    if(session!=null) session.close()
    if(cluster != null) cluster.close();
  }

  def getCluster(): Cluster = {

    val startTime = java.lang.System.currentTimeMillis()
    val cl: Cluster = Cluster.builder()
      .addContactPoints(ips:_*)

      .withCredentials(user.trim(), pass.trim())
      .build()
    val endTime = java.lang.System.currentTimeMillis()
    val interval = endTime - startTime
    log.info("build cassandra cluster takes " + interval + " milliseconds")
    cl
  }
}
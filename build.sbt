name := "Gungnir"

version := "0.6.10"


scalaVersion := "2.12.7"
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.13.0",
  "com.webex.dap.oneportal" % "spark-streaming-job-metrics_cdh-6.3.0_2.11" % "3.0" % "provided" from "file:///"+file("").getAbsolutePath+"/lib/spark-streaming-job-metrics_cdh-6.3.0_2.11-3.0.jar",
  "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",

  //donot enforce the driver here
  //"com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1",
  "commons-configuration" % "commons-configuration" % "1.9",

  //Test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % Test excludeAll( ExclusionRule(organization = "io.netty")),
  "org.apache.spark" %% "spark-hive" % "3.1.1" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.kafka" % "kafka-clients" % "2.7.0" % Test,
  "org.apache.kafka" % "kafka-clients" % "2.7.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.12" % "2.7.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.12" % "2.7.0" % Test ,
  "org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % Test classifier "shaded" excludeAll( ExclusionRule(organization = "io.netty"))
)

libraryDependencies += "org.codehaus.jettison" % "jettison" % "1.3.7"
libraryDependencies += "org.jboss.netty" % "netty" % "3.2.9.Final"


excludeDependencies += "net.jpountz.lz4"

dependencyOverrides ++= Seq(
  "org.apache.kafka" % "kafka_2.12" % "2.7.0" % Test
)

testOptions += Tests.Argument(TestFrameworks.JUnit)

parallelExecution in Test := false

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

unmanagedResourceDirectories in Test += baseDirectory.value / "src" / "main" / "resources"
unmanagedBase := baseDirectory.value / "lib"
jacocoExcludes ++= Seq("com.cisco.gungnir.pipelines.*")
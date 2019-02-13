name := "Gungnir"

version := "0.3.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
  "com.webex.dap.oneportal" % "spark2-streaming-job-metrics_cdh-5.14_2.11" % "3.0" % "provided" from "file:///"+file("").getAbsolutePath+"/lib/spark2-streaming-job-metrics_cdh-5.14_2.11-3.0.jar",
  //Test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test excludeAll( ExclusionRule(organization = "io.netty")),
  "org.apache.spark" %% "spark-hive"       % "2.3.0" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test ,
  "org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % Test classifier "shaded" excludeAll( ExclusionRule(organization = "io.netty")))

excludeDependencies += "net.jpountz.lz4"

dependencyOverrides ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test
)

testOptions += Tests.Argument(TestFrameworks.JUnit)

parallelExecution in Test := false

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

unmanagedResourceDirectories in Test += baseDirectory.value / "src" / "main" / "resources"
unmanagedBase := baseDirectory.value / "lib"
jacocoExcludes ++= Seq("com.cisco.gungnir.pipelines.*")

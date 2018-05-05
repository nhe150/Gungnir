name := "Gungnir"

version := "0.1"

scalaVersion := "2.11.11"
//crossScalaVersions := Seq("2.10.6", "2.11.11","2.12.3")
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7",
  "org.spark-project.spark" % "unused" % "1.0.0" % "provided",
  "com.typesafe" % "config" % "1.3.0",
  //Test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test,
  "org.apache.spark" %% "spark-hive"       % "2.0.0" % Test,
  "com.novocode" % "junit-interface" % "0.10" % Test,
   "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test,
  "org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % Test classifier "shaded",
  "org.hectorclient" % "hector-core" % "2.0-0" % Test
)

dependencyOverrides +=  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test


testOptions += Tests.Argument(TestFrameworks.JUnit)

parallelExecution in Test := false

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

unmanagedResourceDirectories in Test += baseDirectory.value / "src" / "main" / "resources"

jacocoExcludes ++= Seq("SparkDataStreaming", "SparkDataBatch", "SparkDataMonitor")
coverageExcludedPackages := "<empty>;util.Constants;DeleteRocords;"


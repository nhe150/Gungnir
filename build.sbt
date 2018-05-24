name := "Gungnir"

version := "0.2-SNAPSHOT"

scalaVersion := "2.11.11"
val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
  //Test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test,
  "org.apache.spark" %% "spark-hive"       % "2.3.0" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test classifier "test",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test,
  "org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % Test classifier "shaded")

excludeDependencies += "net.jpountz.lz4"

dependencyOverrides +=  "org.apache.kafka" % "kafka_2.11" % "0.11.0.0" % Test

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

jacocoExcludes ++= Seq("com.cisco.gungnir.pipelines.*")

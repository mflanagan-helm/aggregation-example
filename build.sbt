scalaVersion := "2.13.8"
version := "1.0"
libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",

  // kafka
  "org.apache.kafka" % "kafka-clients" % "3.2.1",
  "org.apache.kafka" % "kafka-streams" % "3.2.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.2.1",
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.2.1",

  // json4s
  "org.json4s" %% "json4s-jackson" % "4.0.5",
  "org.json4s" %% "json4s-native" % "4.0.5"
)
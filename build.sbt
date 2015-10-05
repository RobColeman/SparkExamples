name := "sparkExamples"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.4.1"
val sparkProvided = if(sys.env.get("BUILD_ENV") == Some("spark")) "provided" else "compile"

resolvers ++= Seq(
  "Typesafe repository snapshots"    at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases"     at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                    at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"                at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"               at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                 at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository"       at "http://download.java.net/maven/2/",
  "websudos"                         at "http://dl.bintray.com/websudos/oss-releases",
  "Typesafe Repo"                    at "http://repo.typesafe.com/typesafe/releases/",
  "Job Server Bintray"               at "https://dl.bintray.com/spark-jobserver/maven"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion % sparkProvided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkProvided,
  "org.apache.spark" %% "spark-sql"       % sparkVersion % sparkProvided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkProvided,
  "org.apache.spark" % "spark-streaming_2.10" % sparkVersion % sparkProvided,
  "org.apache.spark" % "spark-streaming-kinesis-asl_2.10" % sparkVersion % sparkProvided,
  "org.scalatest" %% "scalatest"          % "2.2.4"       % "test"
)
name := "groundTruthSO"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.4.4",
    "org.apache.spark" %% "spark-core" % "2.4.4",
    "org.apache.spark" %% "spark-mllib" % "2.4.4",
    "org.jsoup" % "jsoup" % "1.12.1"
)

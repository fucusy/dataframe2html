organization := "fucusy"
name := "dataframe2html"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.10"

val sparkVersion  = "2.4.3"

libraryDependencies ++= Seq(
                            "org.scalactic" %% "scalactic" % "3.1.1",
                            "org.scalatest" %% "scalatest" % "3.1.1" % "test",
                            "org.apache.spark"%% "spark-core"      % sparkVersion % "provided",
                            "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided"
)


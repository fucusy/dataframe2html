organization := "io.github.fucusy"
name := "dataframe2html"
version := "0.1.5"
scalaVersion := "2.11.10"

val sparkVersion  = "2.4.3"

libraryDependencies ++= Seq(
                            "org.scalactic" %% "scalactic" % "3.1.1",
                            "org.scalatest" %% "scalatest" % "3.1.1" % "test",
                            "org.apache.spark"%% "spark-core"      % sparkVersion % "provided",
                            "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided"
)


credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")


ThisBuild / organization := "io.github.fucusy"
ThisBuild / organizationName := "Fucusy"
ThisBuild / organizationHomepage := Some(url("https://github.com/fucusy"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/fucusy/dataframe2html"),
    "scm:git@github.com:fucusy/dataframe2html.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "fucusy",
    name  = "Qiang Chen",
    email = "fucus.me@gmail.com",
    url   = url("https://github.com/fucusy")
  )
)

ThisBuild / description := "Make the ability to show the image and the data of dataframe in notebook."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/fucusy/dataframe2html"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true

import xerial.sbt.Sonatype._
publishTo := sonatypePublishToBundle.value
sonatypeBundleDirectory := (ThisBuild / baseDirectory).value / target.value.getName / "sonatype-staging" / s"${version.value}"

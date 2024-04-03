organization := "com.wolt.osm"
homepage := Some(url("https://github.com/woltapp/spark-osm-datasource"))
scmInfo := Some(ScmInfo(url("https://github.com/woltapp/spark-osm-datasource"), "git@github.com:woltapp/spark-osm-datasource.git"))
developers := List(Developer("akashihi",
  "Denis Chaplygin",
  "denis.chaplygin@wolt.com",
  url("https://github.com/akashihi")))
licenses += ("GPLv3", url("https://www.gnu.org/licenses/gpl-3.0.txt"))
publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

name := "spark-osm-datasource"
version := "0.3.0"

scalaVersion := "2.12.19"

scalacOptions := Seq("-unchecked", "-deprecation")

val mavenLocal = "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  "com.wolt.osm" % "parallelpbf" % "0.3.1",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.18" % "it,test",
  "org.scalactic" %% "scalactic" % "3.2.18" % "it,test"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.filterDistinctLines
  case path if path.endsWith(".SF")  => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

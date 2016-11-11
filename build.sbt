import sbt.Keys._

lazy val root = (project in file(".")).
  settings(
    name := "OfflineESIndex",
    version := "2.3.2",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("sk.eset.dbsystems.OfflineESIndexGenerator"),
    exportJars := true,
    retrieveManaged := true
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.8.0" // % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0-cdh5.8.0" // % "provided"

libraryDependencies += "org.apache.spark" % "spark-catalyst_2.10" % "1.6.0-cdh5.8.0" // % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
libraryDependencies += "sk.eset.dbsystems" % "es-shaded" % "1.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.2"

// There is a conflict between Guava/Jackson versions on Elasticsearch and Hadoop
// Shading Guava Package
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename(
   "com.github.scopt.**" -> "shadescopt.@1", 
   "org.joda.**" -> "shadeorgjoda.@1", 
   "com.jsuereth.**" -> "shadejsuereth.@1", 
   "com.fasterxml.**" -> "shadefasterxml.@1",
   "com.google.**" -> "shadegoogle.@1").inAll
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

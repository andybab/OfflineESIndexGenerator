import sbt.Keys._

lazy val root = (project in file(".")).
  settings(
    name := "OfflineESIndex",
    version := "4.0.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("sk.eset.dbsystems.OfflineESIndexGenerator"),
    exportJars := true,
    retrieveManaged := true,
    useCoursier := false
  )

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.4"  % "provided"
libraryDependencies += "com.github.scopt" % "scopt_2.11" % "4.0.1"
libraryDependencies += "sk.eset.dbsystems" % "es-shaded" % "4.1.1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.2"
libraryDependencies += "org.elasticsearch.plugin" % "transport-netty4-client" % "7.12.0"
libraryDependencies += "org.apache.lucene" % "lucene-core" % "8.8.0"

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
  case path if path.contains("META-INF/services") => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard 
  case _ => MergeStrategy.first
}

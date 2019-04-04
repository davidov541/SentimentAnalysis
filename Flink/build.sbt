name := "FlinkTwitterSentiment"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies  ++= Seq(
  "org.apache.flink" %% "flink-scala" % "1.3.2",
  "org.apache.flink" %% "flink-streaming-scala" % "1.3.2",
  "org.apache.flink" %% "flink-clients" % "1.3.2",
  "org.apache.flink" %% "flink-connector-twitter" % "1.3.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Second Server" at "http://repo1.maven.org/maven2",
  "Maven Central Server" at "http://central.maven.org/maven2",
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/",
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)
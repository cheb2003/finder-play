scalaVersion := "2.10.0"

libraryDependencies in ThisBuild ++= Seq(
	"net.sourceforge.jtds" % "jtds" % "1.2.4",
	"com.jolbox" % "bonecp" % "0.7.1.RELEASE",
	"com.typesafe.slick" %% "slick" % "1.0.1",
    "commons-lang" % "commons-lang" % "2.6",
    "org.mongodb" % "casbah_2.10" % "2.6.0",
    "com.typesafe.akka" % "akka-remote_2.10" % "2.1.4",
    "com.typesafe.akka" % "akka-actor_2.10" % "2.1.4",
    "com.typesafe.akka" % "akka-slf4j_2.10" % "2.1.4",
    "com.typesafe" % "config" % "1.0.0",
    "com.typesafe.akka" % "akka-kernel_2.10" % "2.1.4",
    "org.apache.lucene" % "lucene-analyzers-common" % "4.3.0",
    "org.apache.lucene" % "lucene-core" % "4.3.0",
    "com.h2database" % "h2" % "1.3.172"
)

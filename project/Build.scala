import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {


  val appDependencies = Seq(
    // Add your project dependencies here,
    "com.typesafe.slick" %% "slick" % "1.0.1",
    "org.reactivemongo" %% "reactivemongo" % "0.9",
    "org.reactivemongo" %% "play2-reactivemongo" % "0.9",
    "commons-lang" % "commons-lang" % "2.6",
    "org.mongodb" %% "casbah" % "2.6.0",
    "com.typesafe.akka" %% "akka-remote" % "2.1.4",
    "com.typesafe.akka" %% "akka-actor" % "2.1.4",
    "com.typesafe.akka" %% "akka-slf4j" % "2.1.4",
    "com.typesafe" % "config" % "1.0.0",
    "com.typesafe.akka" %% "akka-kernel" % "2.1.4",
    "org.apache.lucene" % "lucene-analyzers-common" % "4.3.0",
    "org.apache.lucene" % "lucene-core" % "4.3.0",
    "org.apache.lucene" % "lucene-queryparser" % "4.3.0",
    "org.specs2" %% "specs2" % "2.0" % "test"
  )
  
  override def settings = super.settings ++ org.sbtidea.SbtIdeaPlugin.settings
  val util = Project(id="util", base=file("util"))
  
  //val index = play.Project("index", "1.0", appDependencies,path = file("index")).settings(
  val index = Project(id="index", base=file("index")).dependsOn(util)
  
  
  val console = play.Project("console", "1.0", appDependencies,path = file("console")).settings(
    
  ).dependsOn(util)

  val search = play.Project("search", "1.0", appDependencies,path = file("search")).settings(
    
  ).dependsOn(util)
  
}

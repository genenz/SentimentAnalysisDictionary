import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList

name := "SentimentAnalysisDictionary"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.0"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
name := "sscheck"

version := "1.0"

scalaVersion := "2.10.5"

lazy val sparkVersion = "1.4.0"

// lazy val specs2Version = "3.6.2" 
  // this version fixes issue #393
lazy val specs2Version = "3.6.2-20150716123420-ac2f605"

// Use `sbt doc` to generate scaladoc, more on chapter 14.8 of "Scala Cookbook"

// if parallel test execution is not disabled and several test suites using
// SparkContext (even through SharedSparkContext) are running then tests fail randomly
parallelExecution := false

// Spark 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion 

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion 

// additional libraries: NOTE as we are writing a testing library they should also be available for main
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2"

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version 

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-junit" % specs2Version 

libraryDependencies += "io.github.nicolasstucki" %% "multisets" % "0.1"

libraryDependencies += "holdenk" % "spark-testing-base" % "1.3.0_0.0.5"

resolvers ++= Seq(
  "MVN Repository.com" at "http://mvnrepository.com/artifact/",
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)


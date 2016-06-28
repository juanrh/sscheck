name := "sscheck"

organization := "es.ucm.fdi"

version := "0.2.4" // "0.2.4-SNAPSHOT" //

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

bintrayPackageLabels := Seq("testing")

bintrayVcsUrl := Some("git@github.com:juanrh/sscheck.git")

lazy val sparkVersion = "1.6.1"

lazy val specs2Version = "3.8.4"

// Use `sbt doc` to generate scaladoc, more on chapter 14.8 of "Scala Cookbook"

// show all the warnings: http://stackoverflow.com/questions/9415962/how-to-see-all-the-warnings-in-sbt-0-11
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

// if parallel test execution is not disabled and several test suites using
// SparkContext (even through SharedSparkContext) are running then tests fail randomly
parallelExecution := false

// Could be interesting at some point
// resourceDirectory in Compile := baseDirectory.value / "main/resources"
// resourceDirectory in Test := baseDirectory.value / "main/resources"

// Configure sbt to add the resources path to the eclipse project http://stackoverflow.com/questions/14060131/access-configuration-resources-in-scala-ide
// This is critical so log4j.properties is found by eclipse
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// additional libraries: NOTE as we are writing a testing library they should also be available for main
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.1"

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-junit" % specs2Version

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"

resolvers ++= Seq(
  "MVN Repository.com" at "http://mvnrepository.com/artifact/",
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

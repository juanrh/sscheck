# sscheck
Utilities for using ScalaCheck with Spark and Spark Streaming, based on Specs2

[Jenkins](https://juanrhcubox.duckdns.org:8080/)

Use linear temporal logic to write ScalaCheck properties for Spark Streaming programs, see the [**Quickstart**](https://github.com/juanrh/sscheck/wiki/Quickstart) for details. See also

 * **scaladoc**
   - [scala 2.10](http://juanrh.github.io/doc/sscheck/scala-2.10/api)
   - [scala 2.11](http://juanrh.github.io/doc/sscheck/scala-2.11/api)
 * sbt dependency 

```scala
lazy val sscheckVersion = "0.2.4"
libraryDependencies += "es.ucm.fdi" %% "sscheck" % sscheckVersion
resolvers += Resolver.bintrayRepo("juanrh", "maven")
```
See latest version in [bintray](https://bintray.com/juanrh/maven/sscheck/view)

# Acknowledgements
This work has been partially supported by MICINN Spanish project StrongSoft (TIN2012-39391-C04-04), by the
Spanish MINECO project CAVI-ART (TIN2013-44742-C4-3-R), and by the Comunidad de Madrid project [N-Greens Software-CM](http://n-greens-cm.org/) (S2013/ICE-2731).

Some parts of this code are based on or have been taken from [Spark Testing Base](https://github.com/holdenk/spark-testing-base) by Holden Karau

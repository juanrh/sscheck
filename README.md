# sscheck
Utilities for using ScalaCheck with Spark and Spark Streaming, based on Specs2

[![Build Status](https://travis-ci.org/juanrh/sscheck.svg?branch=master)](https://travis-ci.org/juanrh/sscheck)

# Quickstart
## Using ScalaCheck with Spark core
[Example property](https://github.com/juanrh/sscheck/blob/d1c1799129475fa18e3231f817cad15badeccf8c/src/test/scala/es/ucm/fdi/sscheck/spark/SharedSparkContextBeforeAfterAllTest.scala):
```scala
def forallRDDGenOfNFreqMean = {
  val freqs = Map(1 -> 0, 4 -> 1)
  val rddSize = 300
  val gRDDFreq = RDDGen.ofN(rddSize, Gen.frequency(freqs.mapValues(Gen.const(_)).toSeq:_*))
  val expectedMean = {
  val freqS = freqs.toSeq
    val num = freqS .map({case (f, v) => v * f}). sum
    val den = freqS .map(_._1). sum
    num / den.toDouble
  }  
  Prop.forAll("rdd" |: gRDDFreq){ rdd : RDD[Int] =>
    rdd.mean must be ~(expectedMean +/- 0.1) 
  }
}. set(minTestsOk = 50).verbose 
```
More details in this [blog post](http://data42.blogspot.com.es/2015/07/property-based-testing-with-spark.html) 

## Using ScalaCheck with Spark Streaming
[Example property](https://github.com/juanrh/sscheck/blob/4d1a0c09c569d6c8281ba07c98a54abd972c9546/src/test/scala/es/ucm/fdi/sscheck/spark/streaming/ScalaCheckStreamingTest.scala):
```scala
def countProp(testSubject : DStream[Double] => DStream[Long]) = 
  DStreamProp.forAllAlways(
    Gen.listOfN(10,  Gen.listOfN(30, arbitrary[Double])))(
    testSubject)( 
    (inputBatch : RDD[Double], transBatch : RDD[Long]) => {
      transBatch.count === 1 and
      inputBatch.count === transBatch.first
    }).set(minTestsOk = 10).verbose     
```
More details in this [blog post](http://data42.blogspot.com.es/2015/08/property-based-testing-with-spark.html)

# Acknowledgements
This work has been partially supported by the project [N-Greens Software-CM](http://n-greens-cm.org/) (S2013/ICE-2731), financed by the regional goverment of Madrid, Spain. 

Some parts of this code are based on [Spark Testing Base](https://github.com/holdenk/spark-testing-base) by Holden Karau

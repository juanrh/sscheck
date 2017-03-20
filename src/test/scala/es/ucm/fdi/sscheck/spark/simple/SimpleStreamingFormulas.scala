package es.ucm.fdi.sscheck.spark.simple

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.matcher.ResultMatchers
import org.scalacheck.Arbitrary.arbitrary
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEach
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}
import org.scalacheck.Gen
import es.ucm.fdi.sscheck.gen.PDStream
import es.ucm.fdi.sscheck.gen.Batch

@RunWith(classOf[JUnitRunner])
class SimpleStreamingFormulas 
  extends org.specs2.Specification 
  with DStreamTLProperty
  with org.specs2.ScalaCheck {
  
   // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(50)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""
    Simple demo Specs2 example for ScalaCheck properties with temporal
    formulas on Spark Streaming programs
      - Given a stream of integers
        When we filter out negative numbers
        Then we get only numbers greater or equal to 
          zero $filterOutNegativeGetGeqZero
      - where time increments for each batch $timeIncreasesMonotonically
      """
      
    def filterOutNegativeGetGeqZero = {
      type U = (RDD[Int], RDD[Int])
      val numBatches = 10
      val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Int]), 
                                numBatches)
      val formula = always(nowTime[U]{ (letter, time) => 
        val (_input, output) = letter
        output should foreachRecord {_ >= 0} 
      }) during numBatches
      
      forAllDStream(
      gen)(
      _.filter{ x => !(x < 0)})(
      formula)
    }.set(minTestsOk = 50).verbose

    def timeIncreasesMonotonically = {
      type U = (RDD[Int], RDD[Int])
      val numBatches = 10
      val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Int]))

      val formula = always(nextTime[U]{ (letter, time) =>
        nowTime[U]{ (nextLetter, nextTime) =>
          time.millis <= nextTime.millis
        }
      }) during numBatches-1

      forAllDStream(
      gen)(
      identity[DStream[Int]])(
      formula)
    }.set(minTestsOk = 10).verbose
}
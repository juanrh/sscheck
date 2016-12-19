package es.ucm.fdi.sscheck.spark.simple

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
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
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
   // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(100)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""
    Simple demo Specs2 example for ScalaCheck properties with temporal
    formulas on Spark Streaming programs
      - where if we generate positive numbers then we always get numbers greater than zero $pending positiveNumbersAndIdAreGtZero
      - where time increments for each batch $timeIncreasesMonotonically
      """
      
    def positiveNumbersAndIdAreGtZero = {
      type U = (RDD[Int], RDD[Int])
      val numBatches = 10
      val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, Gen.choose(1, 1000)), numBatches)
      val formula = always(nowTime[U]{ (letter, time) => 
         letter._2 should foreachRecord { _ > 0} 
      }) during numBatches
      
      forAllDStream(
      gen)(
      identity[DStream[Int]])(
      formula)
  }.set(minTestsOk = 10).verbose
  
    def timeIncreasesMonotonically = {
      type U = (RDD[Int], RDD[Int])
      val numBatches = 10
      val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Int]))
      
      val formula = always(nowTimeF[U]{ (letter, time) => 
        nowTime[U]{ (nextLetter, nextTime) =>
          time.millis < nextTime.millis
        } 
      }) during (numBatches-1)
      
      forAllDStream(
      gen)(
      identity[DStream[Int]])(
      formula)
  }.set(minTestsOk = 10).verbose
    
}
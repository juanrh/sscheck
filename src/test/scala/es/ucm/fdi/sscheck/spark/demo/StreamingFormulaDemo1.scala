package es.ucm.fdi.sscheck.spark.demo

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
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}

@RunWith(classOf[JUnitRunner])
class StreamingFormulaDemo1 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(350)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""
    Simple demo Specs2 example for ScalaCheck properties with temporal
    formulas on Spark Streaming programs
      - where a simple property for DStream.count is a success ${countForallAlwaysProp(_.count)}     
      - where a faulty implementation of the DStream.count is detected ${countForallAlwaysProp(faultyCount) must beFailing}
    """
      
  def faultyCount(ds : DStream[Double]) : DStream[Long] = 
    ds.count.transform(_.map(_ - 1))
      
  def countForallAlwaysProp(testSubject : DStream[Double] => DStream[Long]) = {
    type U = (RDD[Double], RDD[Long])
    val (inBatch, transBatch) = ((_ : U)._1, (_ : U)._2)
    val numBatches = 10
    val formula : Formula[U] = always { (u : U) =>
      transBatch(u).count === 1 and
      inBatch(u).count === transBatch(u).first 
    } during numBatches

    val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Double]), numBatches)
    
    forAllDStream[Double, Long](
      gen)(
      testSubject)(
      formula)
  }.set(minTestsOk = 10).verbose  
  
}
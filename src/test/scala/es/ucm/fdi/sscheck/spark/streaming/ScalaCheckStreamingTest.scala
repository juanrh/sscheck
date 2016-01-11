package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.execute.{AsResult, Result}

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration}
import org.apache.spark.streaming.dstream.DStream

import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.DStreamTLProperty
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._

@RunWith(classOf[JUnitRunner])
class ScalaCheckStreamingTest 
  extends org.specs2.Specification  
  with DStreamTLProperty
  with org.specs2.matcher.ResultMatchers
  with ScalaCheck {
    
  override def sparkMaster : String = "local[5]"
  override def batchDuration = Duration(350)
  override def defaultParallelism = 4  
  
  def is = 
    sequential ^ s2"""
    Simple properties for Spark Streaming
      - where the first property is a success $prop1
      - where a simple property for DStream.count is a success ${countProp(_.count)}
      - where a faulty implementation of the DStream.count is detected ${countProp(faultyCount) must beFailing}
    """    
      
  def prop1 = {
    val batchSize = 30   
    val numBatches = 10 
    val dsgenSeqSeq1 = {
      val zeroSeqSeq = Gen.listOfN(numBatches,  Gen.listOfN(batchSize, 0)) 
      val oneSeqSeq = Gen.listOfN(numBatches, Gen.listOfN(batchSize, 1))
      Gen.oneOf(zeroSeqSeq, oneSeqSeq)  
    } 
    type U = (RDD[Int], RDD[Int])
    
    forAllDStream(
      "inputDStream" |: dsgenSeqSeq1)(
      (inputDs : DStream[Int]) => {  
        val transformedDs = inputDs.map(_+1)
        transformedDs
      })(always ((u : U) => {
          val (inputBatch, transBatch) = u
          inputBatch.count === batchSize and 
          inputBatch.count === transBatch.count and
          (inputBatch.intersection(transBatch).isEmpty should beTrue) and
          ( inputBatch should foreachRecord(_ == 0) or 
            (inputBatch should foreachRecord(_ == 1)) 
          )
        }) during numBatches 
      )}.set(minTestsOk = 10).verbose
      
  def faultyCount(ds : DStream[Double]) : DStream[Long] = 
    ds.count.transform(_.map(_ - 1))
    
  def countProp(testSubject : DStream[Double] => DStream[Long]) = {
    type U = (RDD[Double], RDD[Long])
    val numBatches = 10 
    forAllDStream( 
      Gen.listOfN(numBatches,  Gen.listOfN(30, arbitrary[Double])))(
      testSubject
      )(always ((u : U) => {
         val (inputBatch, transBatch) = u
         transBatch.count === 1 and
         inputBatch.count === transBatch.first
      }) during numBatches
    )}.set(minTestsOk = 10).verbose
    
}

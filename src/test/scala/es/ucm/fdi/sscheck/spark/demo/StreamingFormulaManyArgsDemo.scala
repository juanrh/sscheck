package es.ucm.fdi.sscheck.spark.demo

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEach
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}

import scala.math

/* TODO Add at least tests
 * - 2 inputs that join in a single output
 * - 1 input that generates 2 different outputs
 * - 2 inputs and their independent corresponding outputs
 * 
 * no need for interesting programs, write tests as simple as possible
 * */

@RunWith(classOf[JUnitRunner])
class StreamingFormulaManyArgsDemo 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(200)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""Simple sample sscheck properties with more than 1 input and/or output
      - where we can have 2 inputs and 1 output defined as a combination of both 
      inputs ${maxStreamJoinProp(dstreamsMaxOk)}
     - where a combination of 2 with a faulty transformation ${maxStreamJoinProp(dstreamsMaxWrong) must beFailing} 
    """

  def dstreamsMaxOk(xss: DStream[Int], yss: DStream[Int]) =  
    xss.transformWith(yss, (xs: RDD[Int], ys: RDD[Int]) => 
      xs.context.parallelize(List(math.max(xs.max(), ys.max()))))
  
  def dstreamsMaxWrong(xss: DStream[Int], yss: DStream[Int]) = xss
       
 /* dstreamsMax is expected to be a silly transformationt that returns 
  * a DStream where each batch 
  * has only one point that is the max the elements of both RDDs */
  def maxStreamJoinProp(dstreamsMax: (DStream[Int], DStream[Int]) => DStream[Int]) = {
    type U = (RDD[Int], RDD[Int], RDD[Int])
    val (maxBatchSize, numBatches) = (50, 10)
    def gen(min: Int, max: Int) = BatchGen.always(BatchGen.ofNtoM(1, maxBatchSize, Gen.choose(min, max)), numBatches*2)
    val (gen1, gen2) = (gen(1, 10), gen(20, 30))
    
    val formula = alwaysR[U] { case (xss, yss, rss) => 
      // we know this happens because yss always generates numbers 
      // bigger than those in xss
      (rss.max() must be_>(xss.max())) and
      (rss.max() === yss.max())
    } during numBatches
    forAllDStream[Int, Int, Int](
      gen1, gen2)(
      dstreamsMax)(
      formula)
  }.set(minTestsOk = 10).verbose  
  
//  def faultyCount(ds : DStream[Double]) : DStream[Long] = 
//    ds.count.transform(_.map(_ - 1))
//      
//  def countForallAlwaysProp(testSubject : DStream[Double] => DStream[Long]) = {
//    type U = (RDD[Double], RDD[Long])
//    val (inBatch, transBatch) = ((_ : U)._1, (_ : U)._2)
//    val numBatches = 10
//    val formula : Formula[U] = always { (u : U) =>
//      transBatch(u).count === 1 and
//      inBatch(u).count === transBatch(u).first 
//    } during numBatches
//
//    val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Double]), numBatches)
//    
//    forAllDStream[Double, Long](
//      gen)(
//      testSubject)(
//      formula)
//  }.set(minTestsOk = 10).verbose  
  
}
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
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._

import scala.math

@RunWith(classOf[JUnitRunner])
class StreamingFormulaManyArgsDemo 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(450)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""Simple sample sscheck properties with more than 1 input and/or output
     - where we can have 2 inputs and 1 output defined as a combination of both 
      inputs ${cartesianStreamProp(cartesianDStreamOk)}
     - where a combination of 2 with a faulty transformation ${cartesianStreamProp(cartesianDStreamWrong) must beFailing}
     - where we can have 1 input and 2 outputs defined as a transformation of the input 
     ${dstreamCartesianProp1To2}
      - where we can have 2 input and 2 outputs defined as a transformation of the inputs 
     ${dstreamCartesianProp2To2}
    """
     
  val (maxBatchSize, numBatches) = (50, 10)
  def genAlwaysInts(min: Int, max: Int) = 
    BatchGen.always(BatchGen.ofNtoM(1, maxBatchSize, Gen.choose(min, max)), numBatches)

  def cartesianDStreamOk(xss: DStream[Int], yss: DStream[Int]): DStream[(Int, Int)] = 
    xss.transformWith(yss, (xs: RDD[Int], ys: RDD[Int]) => {
     xs.cartesian(ys)
  })
  
  def cartesianDStreamWrong(xss: DStream[Int], yss: DStream[Int]): DStream[(Int, Int)] =
    xss.map{x => (x,x)}
  
  def cartesianStreamProp(dstreamsCartesian: (DStream[Int], DStream[Int]) => DStream[(Int, Int)]) = {
    type U = (RDD[Int], RDD[Int], RDD[(Int, Int)])
    val (xsMin, xsMax, ysMin, ysMax) = (1, 10, 20, 30)
    
    val formula = alwaysR[U] { case (xs, ys, xsys) => 
     xsys.map(_._1).subtract(xs).count === 0 and
     xsys.map(_._2).subtract(ys).count === 0
    } during numBatches
    forAllDStream21(
      genAlwaysInts(1, 10), 
      genAlwaysInts(20, 30))(
      dstreamsCartesian)(
      formula)
  }.set(minTestsOk = 10).verbose  
  
  def dstreamCartesianProp1To2 = {
    type U = (RDD[Int], RDD[(Int, Int)], RDD[(Int, Int, Int)])
    
    val formula = alwaysR[U] { case (xs, pairs, triples) =>
      pairs.map(_._1).subtract(xs).count === 0 and
      pairs.map(_._2).subtract(xs).count === 0 and
      triples.map(_._1).subtract(xs).count === 0 and
      triples.map(_._2).subtract(xs).count === 0 and
      triples.map(_._3).subtract(xs).count === 0 
    } during numBatches
    forAllDStream12(
      genAlwaysInts(1, 10))(
      _.map{x => (x,x)}, 
      _.map{x => (x,x, x)})(
      formula)
  }.set(minTestsOk = 10).verbose 
  
  def dstreamCartesianProp2To2 = {
    type U = (RDD[Int], RDD[Int], RDD[(Int, Int)], RDD[(Int, Int)])
    
    val formula = alwaysR[U] { case (xs, ys, xsys, ysxs) =>
      xsys.map(_._1).subtract(xs).count === 0 and
      xsys.map(_._2).subtract(ys).count === 0 and
      ysxs.map(_._1).subtract(ys).count === 0 and
      ysxs.map(_._2).subtract(xs).count === 0 
    } during numBatches
    forAllDStream22(
      genAlwaysInts(1, 10), 
      genAlwaysInts(20, 30))(
      (xs, ys) => cartesianDStreamOk(xs, ys), 
      (xs, ys) => cartesianDStreamOk(ys, xs))(
      formula)
  }.set(minTestsOk = 10).verbose 
}
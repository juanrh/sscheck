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

@RunWith(classOf[JUnitRunner])
class StreamingFormulaManyArgsDemo 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(400)
  override def defaultParallelism = 4  

  def is = 
    sequential ^ s2"""Simple sample sscheck properties with more than 1 input and/or output
     - where we can have 2 inputs and 1 output defined as a combination of both 
      inputs ${maxStreamJoinProp(dstreamsMaxOk)}
     - where a combination of 2 with a faulty transformation ${maxStreamJoinProp(dstreamsMaxWrong) must beFailing}
     - where we can have 1 input and 2 outputs defined as a transformation of the input 
     ${dstreamSumAndToStrProp1To2}
      - where we can have 2 input and 2 outputs defined as a transformation of the inputs 
     ${dstreamSumAndToStrProp2To2}
    """
     
  val (maxBatchSize, numBatches) = (50, 10)
  def genAlwaysInts(min: Int, max: Int) = 
    BatchGen.always(BatchGen.ofNtoM(1, maxBatchSize, Gen.choose(min, max)), numBatches)

  def dstreamsMaxOk(xss: DStream[Int], yss: DStream[Int]) =  
    xss.transformWith(yss, (xs: RDD[Int], ys: RDD[Int]) => 
      xs.context.parallelize(List(math.max(xs.max(), ys.max()))))
  
  def dstreamsMaxWrong(xss: DStream[Int], yss: DStream[Int]) = xss
       
 /* dstreamsMax is expected to be a silly transformation that returns 
  * a DStream where each batch 
  * has only one point that is the max the elements of both RDDs */
  def maxStreamJoinProp(dstreamsMax: (DStream[Int], DStream[Int]) => DStream[Int]) = {
    type U = (RDD[Int], RDD[Int], RDD[Int])
    val (gen1, gen2) = (genAlwaysInts(1, 10), genAlwaysInts(20, 30))
    
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
  
  def dstreamSum(xss: DStream[Int]) = 
    xss.transform { xs => xs.context.parallelize(List(xs.reduce(_+_)))}
  
  def dstreamSumStr(xss: DStream[Int]) = 
    dstreamSum(xss).map { _.toString}

  def dstreamSumAndToStrProp1To2 = {
    type U = (RDD[Int], RDD[Int], RDD[String])
    val formula = alwaysR[U] { case (xss, sums, sumsStr) =>
      (sums.count === 1) and (sumsStr.count === 1) and
      (xss.reduce(_+_) === sums.take(1).head) and
      (sumsStr.take(1).head === sums.take(1).head.toString)
    } during numBatches
    forAllDStream[Int, Int, String](
      genAlwaysInts(1, 10))(
      dstreamSum, 
      dstreamSumStr)(
      formula)
  }.set(minTestsOk = 10).verbose 
  
  def dstreamSumAndToStrProp2To2 = {
    type U = (RDD[Int], RDD[Int], RDD[Int], RDD[String])
    val formula = alwaysR[U] { case (xss, yss, maxs, sumsStr) =>
      (maxs.count === 1) and (sumsStr.count === 1) and
      (maxs.max() must be_>(xss.max())) and
      (maxs.max() === yss.max()) and
      (yss.reduce(_+_).toString === sumsStr.take(1).head)
    } during numBatches
    forAllDStream[Int, Int, Int, String](
      genAlwaysInts(1, 10), 
      genAlwaysInts(20, 30))(
      dstreamsMaxOk, 
      (xss, yss) => dstreamSumStr(yss))(
      formula)
  }.set(minTestsOk = 10).verbose 
}
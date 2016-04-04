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
/*    
    List((0,1)).map {case (x, y) => x }
    (0,1) match {case (x : Int, y@u) => x + y }
    import es.ucm.fdi.sscheck.prop.tl.Now
    import org.specs2.execute.{Result}
    val f3 : U => Result = { case (inBatch, transBatch) =>
      transBatch.count === 1  and
      inBatch.count === transBatch.first 
    }
    val formula3 : Formula[U] = always[U] {f3} during 3
    val f4 : Formula[U] = always { u : U => u match {
      case (inBatch, transBatch) =>
        transBatch.count === 1  and
        inBatch.count === transBatch.first
      }
    } during 3
    // note here we need to use the second overload of always
    // that gives type context to the partial function, not
    // the implicit
//    val f5 : Formula[U] = always[U,Result] { _ match {
//      case (inBatch, transBatch) =>
//        transBatch.count === 1  and
//        inBatch.count === transBatch.first
//      }
//    } during 3
    // same as f5
    val f6 = //always[U,Result] { case (inBatch, transBatch) =>
      // I think this doesnt work for always due to resultFunToNow that makes
      // it feasible to use always(Formula) through the conversion of its 
      // argument function into a formula
      alwaysAt[U] { case (inBatch, transBatch) =>
        transBatch.count === 1  and
        inBatch.count === transBatch.first
    } during numBatches
    
    val f5 = always[U, org.specs2.execute.Result] { case (inBatch, transBatch) =>
        inBatch.count === transBatch.first
    } during 3
     
//    val f5b = always{ case (inBatch, transBatch) : U =>
//        inBatch.count === transBatch.first
//    } during 3
    
    import es.ucm.fdi.sscheck.prop.tl.ConvertibleToResult._
    //import es.ucm.fdi.sscheck.prop.tl
    val f7 = allw[U] { case (inBatch, transBatch) =>
        //tl.ConvertibleToResult.toConvertibleToResult(inBatch.count === transBatch.first)
        inBatch.count === transBatch.first
    } during 2
   
   val f72 = allw2[U] { case (inBatch, transBatch) =>
        //tl.ConvertibleToResult.toConvertibleToResult(inBatch.count === transBatch.first)
        inBatch.count === transBatch.first
    } during 2
    
    val fff = resultFunToNow[U, Result]{ case (inBatch, transBatch) =>
        transBatch.count === 1  and
        inBatch.count === transBatch.first
    } 
    val ff = (u : U) => u match {
      case (inBatch, transBatch) =>
        transBatch.count === 1  and
        inBatch.count === transBatch.first
      } */

    
//    val f4 : Formula[U] = always { case (inBatch, transBatch) =>
//        transBatch.count === 1  and
//        inBatch.count === transBatch.first
//      //}
//    } during 3
//    val formula2 : Formula[U] = 
//      //always { case (inBatch, transBatch) : U  =>
//      always { case (inBatch, transBatch)  =>
//      transBatch.count === 1 
//      } during numBatches
    
//    val formula2 : Formula[U] = always { case (inBatch, transBatch) : U =>
//      transBatch.count === 1 and
//      inBatch.count === transBatch.first 
//    } during numBatches
    val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Double]), numBatches)
    
    forAllDStream(
      gen)(
      testSubject)(
      //f6)//
      formula)
  }.set(minTestsOk = 10).verbose  
  
}
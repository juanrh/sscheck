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
import org.apache.spark.streaming.dstream.DStream._

import scalaz.syntax.std.boolean._
    
import es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEach
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._

// FIXME: these raw imports should go away with the DSL
import es.ucm.fdi.sscheck.prop.tl.Solved
import org.scalacheck.Prop


@RunWith(classOf[JUnitRunner])
class StreamingFormulaDemo3 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(200) 
  override def defaultParallelism = 3
  override def enableCheckpointing = true

  def is = 
    sequential ^ s2"""
    Check process to persistently detect and ban bad users
      - where a stateful implementation extracts the banned users correctly ${checkExtractBannedUsersList(listBannedUsers)}
      - where a trivial implementation ${checkExtractBannedUsersList(statelessListBannedUsers) must beFailing}
    """  
  /*
   * 
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 643.0 failed 1 times, most recent failure: Lost 
task 0.0 in stage 643.0 (TID 836, localhost): org.apache.spark.SparkException: RDD transformations and actions can only be invoked 
by the driver, not inside of other transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the 
values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063.

This was due to a bad usage of an RDD matcher, see explanation below
   * */

  
  type UserId = Long
  
  def listBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] = {
   val r = ds.updateStateByKey((flags : Seq[Boolean], maybeFlagged : Option[Unit]) =>
      maybeFlagged match {
        case Some(_) => maybeFlagged  
        case None => flags.contains(false) option {()}
      } 
    ).transform(_.keys)
   r.foreachRDD(rdd => println(s"banned = ${rdd.collect().mkString(",")}"))
   r 
  }
      
  def statelessListBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] = {
   val r = ds.map(_._1)
   r.foreachRDD(rdd => println(s"banned = ${rdd.collect().mkString(",")}"))
   r
  }
    
  def checkExtractBannedUsersList(testSubject : DStream[(UserId, Boolean)] => DStream[UserId]) = {
    val batchSize = 10 //20 
    val (headTimeout, tailTimeout, nestedTimeout) = (10, 10, 5) 
    val (badId, ids) = (Gen.oneOf(-1L, -2L), Gen.choose(1L, 50L))  // (15L, Gen.choose(1L, 50L))   
    val goodBatch = BatchGen.ofN(batchSize, ids.map((_, true)))
    val badBatch = //goodBatch + BatchGen.ofN(1, (badId, false))
      goodBatch + BatchGen.ofN(1, badId.map((_, false)))      
    val gen = BatchGen.until(goodBatch, badBatch, headTimeout) ++ 
               BatchGen.always(Gen.oneOf(goodBatch, badBatch), tailTimeout)
    
    type U = (RDD[(UserId, Boolean)], RDD[UserId])
    val (inBatch, outBatch) = ((_ : U)._1, (_ : U)._2)
    
    val formula : Formula[U] = {
      val badInput = at(inBatch)(_ should existsRecord(_ == (badId, false)))
      val allGoodInputs = at(inBatch)(_ should foreachRecord(_._2 == true))
      val noIdBanned = at(outBatch)(_.isEmpty)
      //val badIdBanned = at(outBatch)(_ should existsRecord(_ == badId))
      
      alwaysLQuant2[U] { case (inBatch, _) =>
        val badIds = inBatch.filter{ case (_, isGood) => ! isGood }. keys
        println(s"found badIds = ${badIds.collect.mkString(",")}")
        always { now[U] { case (_, outBatch) =>
          /*
           * this causes 
           * 
           * org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 643.0 failed 1 times, most recent failure: Lost 
task 0.0 in stage 643.0 (TID 836, localhost): org.apache.spark.SparkException: RDD transformations and actions can only be invoked 
by the driver, not inside of other transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the 
values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063.

				FIXME understand why
           * */
          // FIXME: should be RDD inclusion op, see holden talk on that
          // NOTE  existsRecord calls filter(_ == badIds.take(1)(0)), hence that badIds.take(1) is called 
          // in the closure executed at the workers, instead of in the driver
         //if (badIds.count > 0) outBatch should existsRecord(_ == badIds.take(1)(0)) <-- NOTE here we have a take(1) inside a closure
         //else true
          
          // This is basically the fixed version of above, but avoiding calling actions in the closure
//          if (badIds.count > 0) {
//            // collect called from the driver 
//            val badId = badIds.take(1)(0)
//            outBatch should existsRecord(_ == badId)
//          } else true
          badIds.subtract(outBatch).count === 0 // works ok FIXME see Holden presentation on RDD set operations
        }} during nestedTimeout
      } during tailTimeout

      // FIXME: that formula should be improved for the negative case
      
//      // if first order then we would not have to use a fixed  id for bad user
//      always[U, org.specs2.execute.Result] { case (inBatch, _) =>
//        val badIds = inBatch.filter{ case (_, isGood) => ! isGood }. keys
//        val n = always[U, org.specs2.execute.Result] { case (_, outBatch) =>
//          // note this could fail if badIds is empty: FIXME with implication, lo suyo
//          // seria aqui usar alguna operacion de conjuntos entre RDDs, pero OjO a lo q decia Holden
//          // ver comentarios en milestone de sscheck
//          outBatch should existsRecord(_ == badIds.take(1))
//        } during 3
//        // no puedo devolver n pq este overload de always construye un Always(Now(???)) y 
//        // Now solo acepta convertibles a Result en el resultado, pero no formulas
//        //true
//        n // esto cuela ahora pq he definido formulaToResult, q no esta nada claro q hace todavia, y q daria el primer orden
//      }
//      
    //  ( ( allGoodInputs and noIdBanned ) until badIdBanned on headTimeout ) and
    //  ( always { badInput ==> (always(badIdBanned) during nestedTimeout) } during tailTimeout )  
    }  
    
    forAllDStream(    
      gen)(
      testSubject)( 
      formula)
  }.set(minTestsOk = 2).verbose  
  
  
}
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

@RunWith(classOf[JUnitRunner])
class StreamingFormulaDemo2 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(500) 
  override def defaultParallelism = 3
  override def enableCheckpointing = true

  def is = 
    sequential ^ s2"""
    Check process to persistently detect and ban bad users
      - where a stateful implementation extracts the banned users correctly ${checkExtractBannedUsersList(listBannedUsers)}
      - where a trivial implementation ${checkExtractBannedUsersList(statelessListBannedUsers) must beFailing}
    """
  type UserId = Long
  
  def listBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] = 
    ds.updateStateByKey((flags : Seq[Boolean], maybeFlagged : Option[Unit]) =>
      maybeFlagged match {
        case Some(_) => maybeFlagged  
        case None => flags.contains(false) option {()}
      } 
    ).transform(_.keys)
      
  def statelessListBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] =
    ds.map(_._1)
    
  def checkExtractBannedUsersList(testSubject : DStream[(UserId, Boolean)] => DStream[UserId]) = {
    val batchSize = 20 
    val (headTimeout, tailTimeout, nestedTimeout) = (10, 10, 5) 
    val (badId, ids) = (15L, Gen.choose(1L, 50L))   
    val goodBatch = BatchGen.ofN(batchSize, ids.map((_, true)))
    val badBatch = goodBatch + BatchGen.ofN(1, (badId, false))
    val gen = BatchGen.until(goodBatch, badBatch, headTimeout) ++ 
               BatchGen.always(Gen.oneOf(goodBatch, badBatch), tailTimeout)
    
    type U = (RDD[(UserId, Boolean)], RDD[UserId])
    val (inBatch, outBatch) = ((_ : U)._1, (_ : U)._2)
    
    val formula : Formula[U] = {
      val badInput = at(inBatch)(_ should existsRecord(_ == (badId, false)))
      val allGoodInputs = at(inBatch)(_ should foreachRecord(_._2 == true))
      val noIdBanned = at(outBatch)(_.isEmpty)
      val badIdBanned = at(outBatch)(_ should existsRecord(_ == badId))
      
      ( ( allGoodInputs and noIdBanned ) until badIdBanned on headTimeout ) and
      ( always { badInput ==> (always(badIdBanned) during nestedTimeout) } during tailTimeout )  
    }  
    
    forAllDStream(    
      gen)(
      testSubject)( 
      formula)
  }.set(minTestsOk = 10).verbose  
  
  
}
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
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty,Now}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._

@RunWith(classOf[JUnitRunner])
class StreamingFormulaQuantDemo 
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  override def batchDuration = Duration(350) 
  override def defaultParallelism = 3
  override def enableCheckpointing = true

  def is = 
    sequential ^ s2"""
    Check process to persistently detect and ban bad users
      - where a stateful implementation extracts the banned users correctly ${checkExtractBannedUsersList(listBannedUsers)}
      - where a trivial implementation ${checkExtractBannedUsersList(statelessListBannedUsers) must beFailing}
    """  
  type UserId = Long
  
  def listBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] = {
    val bannedUsers = ds.updateStateByKey((flags : Seq[Boolean], maybeFlagged : Option[Unit]) =>
      maybeFlagged match {
        case Some(_) => maybeFlagged  
        case None => flags.contains(false) option {()}
      } 
    ).transform(_.keys)
    bannedUsers.foreachRDD(rdd => println(s"banned = ${rdd.collect().mkString(",")}"))
    bannedUsers
  }
      
  def statelessListBannedUsers(ds : DStream[(UserId, Boolean)]) : DStream[UserId] = {
    val banned = ds.map(_._1)
    banned.foreachRDD(rdd => println(s"banned = ${rdd.collect().mkString(",")}"))
    banned
  }
    
  def checkExtractBannedUsersList(testSubject : DStream[(UserId, Boolean)] => DStream[UserId]) = {
    val batchSize = 10 //20 
    val (headTimeout, tailTimeout, nestedTimeout) = (10, 10, 5) 
    val (badId, ids) = (Gen.oneOf(-1L, -2L), Gen.choose(1L, 50L)) 
    val goodBatch = BatchGen.ofN(batchSize, ids.map((_, true)))
    val badBatch = goodBatch + BatchGen.ofN(1, badId.map((_, false)))      
    val gen = BatchGen.until(goodBatch, badBatch, headTimeout) ++ 
               BatchGen.always(Gen.oneOf(goodBatch, badBatch), tailTimeout)
    
    type U = (RDD[(UserId, Boolean)], RDD[UserId])
    val (inBatch, outBatch) = ((_ : U)._1, (_ : U)._2)
     
    val badIdsAreAlwaysBanned = alwaysF[U]{ case (inBatch, _) =>
      val badIds = inBatch.filter{ case (_, isGood) => ! isGood }. keys
      println(s"found badIds = ${badIds.collect.mkString(",")}")
      alwaysR[U]{ case (_, outBatch) =>
        badIds.subtract(outBatch) isEmpty
      } during nestedTimeout
    } during tailTimeout
    
    // Same as badIdsAreAlwaysBanned but using at(), just to check the syntax is ok
    // and we can always use at(), for which we also need the trick
    // to avoid override, so it ends up being similar to using several nows, 
    // because at() is most effective when combined with _ for simple assertions
    val badIdsAreAlwaysBannedAt = always{ atF(inBatch){ inBatch => 
      val badIds = inBatch.filter{ case (_, isGood) => ! isGood }. keys
      always{ at(outBatch){outBatch =>
          badIds.subtract(outBatch) isEmpty
      }} during nestedTimeout
    }} during tailTimeout
    
    // Same as badIdsAreAlwaysBanned but using overloads of now(), just to check the syntax is ok
    val badIdsAreAlwaysBannedNow : Formula[U] = {      
      always { nowF[U] { case (inBatch, _) =>
        val badIds = inBatch.filter{ case (_, isGood) => ! isGood }. keys
        always { now[U] { case (_, outBatch) =>
          badIds.subtract(outBatch) isEmpty
        }} during nestedTimeout
      }} during tailTimeout
    }
    
    // trivial example just to check that we can use the time in formulas
     val timeAlwaysIncreases = always( nowTimeF[U]{ (_atoms1, t1) => 
      always( nowTime[U]{ (_atoms2, t2) => 
        println(s"time $t2 should be greater than time $t1")
        t2.millis must beGreaterThan(t1.millis)
      }) during nestedTimeout
    }) during tailTimeout      
        
    forAllDStream[(UserId, Boolean), UserId](    
      gen)(
      testSubject)( 
      badIdsAreAlwaysBanned and
      badIdsAreAlwaysBannedAt and
      badIdsAreAlwaysBannedNow and 
      timeAlwaysIncreases)
  }.set(minTestsOk = 15).verbose  
}

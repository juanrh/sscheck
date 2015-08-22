package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 
import org.specs2.execute.{AsResult,Result}

import org.apache.spark.streaming.Duration
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Queue

import com.typesafe.scalalogging.slf4j.Logging

// sbt "test-only es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEachTest"

@RunWith(classOf[JUnitRunner])
class SharedStreamingContextBeforeAfterEachTest 
  extends org.specs2.Specification 
  with org.specs2.matcher.MustThrownExpectations 
  with org.specs2.matcher.ResultMatchers
  with SharedStreamingContextBeforeAfterEach
  with Logging {
  
  // Spark configuration
  override def sparkMaster : String = "local[5]"
  override def batchDuration = Duration(250) 
  override def defaultParallelism = 3
  
  def is = 
    sequential ^ s2"""
    Simple test for SharedStreamingContextBeforeAfterEach 
      where a simple queueStream test must be successful $successfulSimpleQueueStreamTest
      where a simple queueStream test can also fail $failingSimpleQueueStreamTest
    """      
      
  def successfulSimpleQueueStreamTest = simpleQueueStreamTest(expectedCount = 0)
  def failingSimpleQueueStreamTest = simpleQueueStreamTest(expectedCount = 1) must beFailing
        
  def simpleQueueStreamTest(expectedCount : Int) : Result = {
    val record = "hola"
    val batches = Seq.fill(5)(Seq.fill(10)(record))
    val queue = new Queue[RDD[String]]
    queue ++= batches.map(batch => sc.parallelize(batch, numSlices = defaultParallelism))
    val inputDStream = ssc.queueStream(queue, oneAtATime = true)
    val sizesDStream = inputDStream.map(_.length)
    
    var batchCount = 0
    // NOTE wrapping assertions with a Result object is needed
    // to avoid the Spark Streaming runtime capturing the exceptions
    // from failing assertions
    var result : Result = ok
    inputDStream.foreachRDD { rdd =>
      batchCount += 1
      println(s"completed batch number $batchCount: ${rdd.collect.mkString(",")}")
      result = result and AsResult {
        rdd.filter(_!= record).count() === expectedCount
        rdd.collect{case record => true}.toLocalIterator.hasNext must beTrue
      }
    }
    sizesDStream.foreachRDD { rdd =>
      result = result and AsResult {
        rdd.filter(_!= record.length).count() === 0
      }
    }
    
    // only start the dstream after all the transformations and actions have been defined
    ssc.start()
    
    // wait for completion of batches.length batches
    StreamingContextUtils.awaitForNBatchesCompleted(batches.length)(ssc)
    
    result
  }
}
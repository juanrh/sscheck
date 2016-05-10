package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 
import org.specs2.execute.Result

import org.apache.spark.streaming.Duration
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Queue
import scala.concurrent.duration._

import org.slf4j.LoggerFactory

import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._

// sbt "test-only es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEachTest"

@RunWith(classOf[JUnitRunner])
class SharedStreamingContextBeforeAfterEachTest 
  extends org.specs2.Specification 
  with org.specs2.matcher.MustThrownExpectations 
  with org.specs2.matcher.ResultMatchers
  with SharedStreamingContextBeforeAfterEach {
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  @transient private val logger = LoggerFactory.getLogger("SharedStreamingContextBeforeAfterEachTest")
  
  // Spark configuration
  override def sparkMaster : String = "local[5]"
  override def batchDuration = Duration(250) 
  override def defaultParallelism = 3
  override def enableCheckpointing = false // as queueStream doesn't support checkpointing 
  
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
      result = result and {
        rdd.filter(_!= record).count() === expectedCount
        rdd should existsRecord(_ == "hola")
      }
    }
    sizesDStream.foreachRDD { rdd =>
      result = result and { 
        rdd should foreachRecord(record.length)(len => _ == len)      
      }
    }
    
    // should only start the dstream after all the transformations and actions have been defined
    ssc.start()
    
    // wait for completion of batches.length batches
    StreamingContextUtils.awaitForNBatchesCompleted(batches.length, atMost = 10 seconds)(ssc)
    
    result
  }
}
 
 

  

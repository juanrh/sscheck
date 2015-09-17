package es.ucm.fdi.sscheck.spark.streaming

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import ExecutionContext.Implicits.global
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}

// Inline alternative implementation based on SyncVar
object StreamingContextUtils {
  /** This is a blocking call that awaits (with scala.concurrent.Await.result) for the receiver 
   *  of the input Streaming Context to complete start. This can be used to avoid sending data 
   *  to a receiver before it is ready. 
   * */  
  def awaitUntilReceiverStarted(atMost : scala.concurrent.duration.Duration = 2 seconds)
                               (ssc : StreamingContext): Unit = {
    val receiverStartedPromise = Promise[Unit]
    // val sv = new SyncVar[Unit]
    ssc.addStreamingListener(new StreamingListener {
      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) : Unit = {
        receiverStartedPromise success ()
        // sv.put(())
      }
    })
    // sv.get(atMost toMillis)
    Await.result(receiverStartedPromise.future, atMost)
  }
  
  /** This is a blocking call that awaits for completion of numBatches in ssc. 
   *  NOTE if a receiver is used awaitUntilReceiverStarted() should be called before
   *  
   *  @param atMost maximum amount of time to spend waiting for all the batches to complete
   * */
  def awaitForNBatchesCompleted(numBatches : Int, atMost : scala.concurrent.duration.Duration = 30 seconds)
                               (ssc : StreamingContext) : Unit = {
    val onBatchCompletedSyncVar = new SyncVar[Unit]
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) : Unit =  {  
        if (! onBatchCompletedSyncVar.isSet) {
          // note only this threads makes puts, so no problem with concurrency
          onBatchCompletedSyncVar.put(())
        }
      }
    })
    val waitingForBatches = Future {
      for (_ <- 1 to numBatches) {
        onBatchCompletedSyncVar.take()
      }  
    }
    Await.result(waitingForBatches, atMost)
  }
}
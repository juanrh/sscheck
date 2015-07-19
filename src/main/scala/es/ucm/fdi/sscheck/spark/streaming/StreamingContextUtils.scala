package es.ucm.fdi.sscheck.spark.streaming

import scala.concurrent._
import scala.concurrent.duration._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}

// Inline alternative implementation based on SyncVar
object StreamingContextUtils {
  /** This is a blocking call that awaits (with scala.concurrent.Await.result) for the receiver 
   *  of the input Streaming Context to complete start. This can be used to avoid sending data 
   *  to a receiver before it is ready. 
   * */  
  def awaitUntilReceiverStarted(ssc : StreamingContext, 
                                atMost : scala.concurrent.duration.Duration = 2 seconds) : Unit = {
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
}
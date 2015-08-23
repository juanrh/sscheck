package es.ucm.fdi.sscheck.spark.streaming.receiver

import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import scala.reflect._

import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}
import akka.actor.{Actor, Props, ActorSelection}

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Try

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/** A pair of an actor input DStream and an Akka actor selection that can be used to 
 *  send messages to that actor, that will forward those messages to the input DStream   
 * */
case class InputDStreamWithProxyActor[A:ClassTag](dstream : InputDStream[A], actor : ActorSelection)
object InputDStreamWithProxyActor {
  def apply[A](ssc : StreamingContext, receiverActorName : String)
              (implicit aCt : ClassTag[A]) : InputDStreamWithProxyActor[A] = 
    new InputDStreamWithProxyActor(dstream=ProxyReceiverActorDemo.createActorDStream(ssc, receiverActorName), 
                                   actor=ProxyReceiverActorDemo.getActorSelection(ssc.sparkContext, receiverActorName))
  
  def apply[A](receiverActorName : String)
              (implicit ssc : StreamingContext, aCt : ClassTag[A]) : InputDStreamWithProxyActor[A] =
    apply(ssc, receiverActorName)
}

/** Simple Akka actor that can be used to create an InputDStream, to which 
 *  the actor forwards all the messages it receives that match its generic type 
 * */
class ProxyReceiverActorDemo[A:ClassTag]
  extends Actor 
  with ActorHelper {
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  //@transient private[this] val logger = Logger(LoggerFactory.getLogger("ProxyReceiverActorDemo"))
  @transient private val logger = Logger(LoggerFactory.getLogger("ProxyReceiverActorDemo"))
  
  // Note ActorHelper has Spark's Logging as supertype 
  override def preStart = {
    logger.info(s"Starting ${this.getClass.getName} $self")
  }
  
  override def postStop = {
    logger.info(s"Stopped ${this.getClass.getName} $self")
  } 
  
  override def receive = {
    case msg : A => {
      /* According to the logs and DStream.print there is a delay of between one and two 
       * batches from the call to store and the message appearing in a batch 
       * */
      logger.info(s"received message [${msg}]")
      store(msg)
    }
  }
}
object ProxyReceiverActorDemo {
  def createActorDStream[A](ssc : StreamingContext, receiverActorName : String)
                           (implicit aCt : ClassTag[A]) : InputDStream[A] =
    ssc.actorStream[A](Props(new ProxyReceiverActorDemo[A]), receiverActorName)
    
  def createActorDStream[A](receiverActorName : String)
                           (implicit ssc : StreamingContext, aCt : ClassTag[A]) : DStream[A] =
    createActorDStream(ssc, receiverActorName)(aCt)
    
  def getActorSelection(sc : SparkContext, receiverActorName : String) : ActorSelection = {
    // could use SparkEnv.get.conf instead, but doing this way hoping in the future we 
    // could have several Spark Contexts in the same JVM, with different actor systems accessible
    // through each SparkContext
    val driverHost = sc.getConf.get("spark.driver.host")
    val driverPort = sc.getConf.get("spark.driver.port")
    val actorSystem = SparkEnv.get.actorSystem
    val actorUrl = s"akka.tcp://sparkDriver@$driverHost:$driverPort/user/Supervisor0/$receiverActorName"
    actorSystem.actorSelection(actorUrl)
  }
}

object ReceiverActorDemo 
  extends App {
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  //@transient private[this] val logger = Logger(LoggerFactory.getLogger("ReceiverActorDemo"))
  @transient private val logger = Logger(LoggerFactory.getLogger("ReceiverActorDemo"))
  
  val conf = new SparkConf().setMaster("local[5]").setAppName("ReceiverActorDemo")    
  val sc = new SparkContext(conf)
  val batchDuration = Duration(100)
  val ssc = new StreamingContext(sc, batchDuration)
  
  // get reference to receiver actor so we can send messages to it
  val receiverActorName = "ReceiverActorFoo"
  val inputDStreamWithProxyActor = InputDStreamWithProxyActor[String](ssc, receiverActorName)
  val receiverActor = inputDStreamWithProxyActor.actor 
  
  /* with this inputDStream : ReceiverInputDStream[Nothing] and we get SparkDriverExecutionException: Execution error
   * Caused by: java.lang.ArrayStoreException: [Ljava.lang.Object; 
   * 
   * val inputDStream = ssc.actorStream(Props(new ProxyReceiverActor[String]), receiverActorName)
   */
  // with this inputDStream : ReceiverInputDStream[String]
  // val inputDStream = ssc.actorStream[String](Props(new ProxyReceiverActor[String]), receiverActorName)
  val inputDStream = inputDStreamWithProxyActor.dstream
  inputDStream. print
  ssc.addStreamingListener(new StreamingListener {
    override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) : Unit = {
      // run this is a future to avoid blocking the listener handler loop, as this
      // code has sleeps and so it's quite slow
      future {
        // only start sending the messages when the receiver has started, otherwise the
        // first messages can be lost
        logger.warn("now receiver is ready to receive messages")
      
        // this message doesn't reach the DStream because it doesn't 
        // have the type String, so it is discarded in the receive of the actor   
        receiverActor ! 42
        for (msg <- "hola caracola que tal lo llevas yo aqui intentando usar actores con Spark Streaming, parece que funciona ok!".split("""\s+""")) {
          // these messages reach the DStream because it has type String
          logger.info(s"sending message [${msg}] to receiver actor $receiverActor")
          receiverActor ! msg
	      Thread.sleep(50)
        }
      }
    }
  })
  
  // ssc.start()
  // Thread.sleep(2 * 1000)
  // StreamingListener registered after the Streaming Context has started
  // are ignored
  
  // let this run for some batches
  val maxNumBatches = 15 // 5 // this also stops ok before all the messages can even be sent
  ssc.addStreamingListener(new StreamingListener {
    var hasReceiverStarted = false
    var batchesCounter = 0
    var stoppingContext = false
    
    override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) : Unit = {
      hasReceiverStarted = true
    }
    
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) : Unit =  {
      if (hasReceiverStarted) {
        logger.info(s"batch completed at time ${batchCompleted.batchInfo.batchTime}")
        batchesCounter += 1
        // ssc.stop is ignored if executed before the receiver has started, so we start
        // counting batches only after the receiver has started
        if (batchesCounter >= maxNumBatches && ! stoppingContext) {
          // graceful stop can take some batches, use stoppingContext to avoid calling stop several times 
          stoppingContext = true
          logger.warn(s"stopping streaming context at time ${batchCompleted.batchInfo.batchTime}")
          // we need another thread to stop the context, otherwise we get a deadlock as 
          // this method cannot finish until the context stops, and the context cannot
          // stop until this method finishes
          future {
            Try { ssc.stop(stopSparkContext=true, stopGracefully=true) } recover {
              case _ => {
                logger.error(s"forcing non graceful stop of streaming context")
                ssc.stop(stopSparkContext=true, stopGracefully=false) 
              }
            }
          }
        }
      }
    }
  })
  
  ssc.start()
  ssc.awaitTermination()
}

/*
TODO: register a listener for batch complete, wait for the first batch end, 
and send data for each new batch
*/

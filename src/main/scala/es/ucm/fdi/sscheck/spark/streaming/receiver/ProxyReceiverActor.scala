package es.ucm.fdi.sscheck.spark.streaming.receiver

import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream}
import scala.reflect._

import org.apache.spark.streaming.receiver.ActorHelper
import akka.actor.{Actor, Props, ActorSelection}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

// TODO: consider buffering to call the reliable version of store with a bunch of data
/** Simple Akka actor that can be used to create an InputDStream, to which 
 *  the actor forwards all the messages it receives that match its generic type
 *  
 *  There is a delay of between one and two batches from the call to store and 
 *  the message appearing in a batch
 * */
class ProxyReceiverActor[A:ClassTag]
  extends Actor 
  with ActorHelper {  
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  // @transient private[this] val logger = Logger(LoggerFactory.getLogger("ProxyReceiverActor"))
  @transient private val logger = Logger(LoggerFactory.getLogger("ProxyReceiverActor"))
  
  override def preStart = {
    logger.info(s"Starting $self")
  }
  
  override def postStop = {
    logger.info(s"Stopped $self")
  } 
  
  override def receive = {
    /* Akka converts scala.Int into java.lang.Integer, that is subclass of AnyRef 
     * but not of scala.Int, therefore a check like that would imply dropping any 
     * Int that is sent, which is not an option
     * case msg : A => store(msg) 
     */
    case msg => {
      // logger.debug(s"received message [${msg}] with type ${msg.getClass} at actor ${self}") 
      
      /* other option is http://jatinpuri.com/2014/03/replace-view-bounds/, but 
       "no implicit view available" seems to scape from Try */
      // Try(msg.asInstanceOf[A])       // check convertible
      //  .foreach {x : A => super[ActorHelper].store(x)} // store converted
      // Let it crash version
      super[ActorHelper].store(msg.asInstanceOf[A])
    }
  }
}

/* TODO: coud use actor lookup here insted of a hard coded path
 * */
object ProxyReceiverActor {
  def createActorDStream[A](ssc : StreamingContext, receiverActorName : String)
                           (implicit aCt : ClassTag[A]) : InputDStream[A] =
    ssc.actorStream[A](Props(new ProxyReceiverActor[A]), receiverActorName)
    
  def createActorDStream[A](receiverActorName : String)
                           (implicit ssc : StreamingContext, aCt : ClassTag[A]) : InputDStream[A] =
    createActorDStream(ssc, receiverActorName)(aCt)
    
  def getActorSelection(receiverActorName : String)
  					  (implicit sc : SparkContext): ActorSelection =
    getActorSelection(sc, receiverActorName)
    
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
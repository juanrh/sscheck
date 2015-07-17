package es.ucm.fdi.sscheck.spark.streaming.receiver

import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream}
import scala.reflect._

import org.apache.spark.streaming.receiver.ActorHelper
import akka.actor.{Actor, Props, ActorSelection}

/** Simple Akka actor that can be used to create an InputDStream, to which 
 *  the actor forwards all the messages it receives that match its generic type
 *  
 *  There is a delay of between one and two batches from the call to store and 
 *  the message appearing in a batch
 * */
class ProxyReceiverActor[A:ClassTag]
      extends Actor with ActorHelper with Logging  {  
  
  override def preStart = {
    logInfo(s"Starting $self")
  }
  
  override def postStop = {
    logInfo(s"Stopped $self")
  } 
  
  override def receive = {
    // case msg : A => store(msg) <= this check implies primitive types like Int are dropped
    case msg => {
      // logInfo(s"received message $msg")// FIXME
      store(msg.asInstanceOf[A])
    }
  }
}
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
    // TODO: coud use actor lookup here insted of a hard coded path
    val actorUrl = s"akka.tcp://sparkDriver@$driverHost:$driverPort/user/Supervisor0/$receiverActorName"
    actorSystem.actorSelection(actorUrl)
  }
}
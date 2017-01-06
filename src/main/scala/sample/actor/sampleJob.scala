package sample.actor
/**
  * Created by marcin on 2016-11-26.
  */

//http://bytes.codes/2013/01/17/Distributing_Akka_Workloads_And_Shutting_Down_After/
//http://stackoverflow.com/questions/22383939/scala-read-file-process-lines-and-write-output-to-a-new-file-using-concurrent
//http://doc.akka.io/docs/akka/2.4/scala/stream/stream-quickstart.html
//https://ivanyu.me/blog/2016/12/12/about-akka-streams/#more-983
//https://tech.smartling.com/crawling-the-web-with-akka-streams-60ed8b3485e4#.18grzjuho
//https://github.com/akka/akka/issues/20031
//http://liferepo.blogspot.se/2015/01/creating-reactive-streams-components-on.html
//https://github.com/CogniStreamer/serilog-sinks-akkaactor
//https://ivanyu.me/docs/alpakka/latest/file.html/blog/2016/12/12/about-akka-streams/#more-983
//http://doc.akka.io/docs/akka/current/scala/stream/stream-integrations.html#Sink_actorRefWithAck
//http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/
import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, _}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.stream.actor.{ActorSubscriber, OneByOneRequestStrategy, RequestStrategy}
import akka.stream.javadsl.Framing
import akka.stream.scaladsl.{FileIO, _}
import akka.stream.{ActorMaterializer, IOResult, OverflowStrategy}
import akka.util.{ByteString, Timeout}
import breeze.linalg.{SliceVector, sum, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.numerics.{exp, sigmoid, sqrt, log => ln}
import breeze.stats.distributions._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
object Types {
  // type Activation = BDV[Double]
  type ActivationFunction = BDV[Double] => BDV[Double]
  //type WeightType = BDM[Double]
  //type Delta = BDV[Double]
  //type SubGradientType = BDV[Double]
  //type GradientType = BDV[Double]
  //type VT = BDV[Double]
}

object HelloAkkaScala extends App {

  val customConf = ConfigFactory.parseString("""
   akka {
   # JVM shutdown, System.exit(-1), in case of a fatal error,
   # such as OutOfMemoryError
   jvm-exit-on-fatal-error = on
   # Toggles whether threads created by this ActorSystem should be daemons or not
   daemonic = off
   actor {
     actor.provider = "akka.remote.RemoteActorRefProvider"
     typed {
       # Default timeout for typed actor methods with non-void return type
       timeout = 5s
     }
     creation-timeout = 90s
   }
   remote {
     enabled-transports = ["akka.remote.netty.tcp"]
     netty {
       hostname = ""
       port = 2552
       # (O) Sets the connectTimeoutMillis of all outbound connections,
       # i.e. how long a connect may take until it is timed out
       connection-timeout = 120s
     }
     server-socket-worker-pool {
         # Min number of threads to cap factor-based number to
         pool-size-min = 2

         # The pool size factor is used to determine thread pool size
         # using the following formula: ceil(available processors * factor).
         # Resulting size is then bounded by the pool-size-min and
         # pool-size-max values.
         pool-size-factor = 1.0

         # Max number of threads to cap factor-based number to
         pool-size-max = 8
       }
     untrusted-mode = off
   }
   enable-additional-serialization-bindings = on
   serializers {
       proto = "akka.remote.serialization.ProtobufSerializer"
       daemon-create = "akka.remote.serialization.DaemonMsgCreateSerializer"
   }
   serialization-bindings {
       "java.io.Serializable" = none
       "com.google.protobuf.GeneratedMessage" = proto
       "akka.remote.DaemonMsgCreate" = daemon-create
     }
 }
          """)
  // ConfigFactory.load sandwiches customConfig between default reference
  // config and default overrides, and then resolves it.
  implicit val system = ActorSystem("HelloSystem", ConfigFactory.load(customConf))


  //val file_name:String = "/home/marcin/IdeaProjects/AkkaDistBelief/src/main/resources/bigdata.tr.txt"



  val ModelMaster = system.actorOf(Props(new Master(
      numberOfShards = 8
    , activation = (x: BDV[Double]) => x.map(el => sigmoid(el))
    , activationDerivative = (x: BDV[Double]) => x.map(el => sigmoid(el) * (1 - sigmoid(el)))
    //eta = 0.5
     )
    )
  )


  implicit val materializer = ActorMaterializer()
  def lineToFFMRow(line:String) :FFMRow ={
    val splitted = line.split(" ")
    val y = splitted(0).toDouble // 1.0 else -1.0
    val FFMNodeVec:BV[FFMNode] = new BDV(splitted.slice(1,splitted.size).map(x=>{
      val data = x.split(":")
      FFMNode(data(0).toInt, data(1).toInt, data(2).toDouble)
    }).toArray)

    FFMRow(y, FFMNodeVec)
  }

  val file_name:String = "/media/small_ssd/libffm-local_train/part-00000"
  val file_source:Source[FFMRow, Future[IOResult]] = FileIO.fromPath(Paths.get(file_name)) //fromPath since 2.4.5
    //.via(Compression.gunzip())
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .map(x => lineToFFMRow(x))

  file_source.to(Sink.actorRefWithAck(ModelMaster
    ,onInitMessage= Master.Init
    ,ackMessage= Master.Ack
    ,onCompleteMessage= Master.Complete
    ,onFailureMessage= Status.Failure
    //,onFailureMessage:(Throwable)
  )//.withAttributes(inputBuffer(100, 100))
  ).run()

  val terminator = system.actorOf(Props(new Terminator(ModelMaster)), "app-terminator")

} //HelloAkkaScala

class Terminator(app: ActorRef) extends Actor with ActorLogging {
  context watch app
  app ! Master.InitParameters
  log.info("Started")
  def receive: Receive = {
    case Master.JobDone => {log.info("Finished Computing!!!!!");context.stop(self)}
  }
}

case class Observation(x: BDV[Double], y: BDV[Double])

// Master messages that say what happens.
object Master {

  case class Done(dataShardId: Int)

  case object Start

  case object JobDone

  case object Init

  case object Ack

  case object Complete

  case object Failure

  case class ShardRequest(x:Int)

  case object InitParameters
}

class Master(numberOfShards:Int, activation: Types.ActivationFunction,
             activationDerivative: Types.ActivationFunction
             ) extends ActorSubscriber with ActorLogging {
  override protected def requestStrategy: RequestStrategy = OneByOneRequestStrategy
           protected def overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure
  //WatermarkRequestStrategy
     val actorName = "Master"
  implicit val timeout = Timeout(15.second)

  val k:Int = 2
  val l:Int = 43570983
  val parametersActor = context.actorOf(Props(
      new Parameters(eta = 0.1
        , lambda = 0.00002
        , l = l // number of lines in a file
        , n = 19784669 // number of different values
        , m = 23 // number of fields
        , k = k
        , weightsInitDist = breeze.stats.distributions.Uniform(0, 1), epochs = 1
      ))
    )


  //weightsInitDist = breeze.stats.distributions.Gaussian(0, 1)

  log.info(s"Parameters actor initiated.")
  val dataShards = (1 to numberOfShards).toSeq

  val lossTrackerActor: ActorRef = context.actorOf(Props(new Loss(l)))

  val dataShardActors: ActorRef =
    context.actorOf(RoundRobinPool(numberOfShards).props(Props(new DataShard(
      //shardId = dataShard._2
      //shardManager = shardManager
      //, trainingData = dataShard._1
        activation = activation
      , activationDerivative = activationDerivative
      , parametersActor = parametersActor
      , k = k
      , lossTracker = lossTrackerActor
    ))), "router2")

  log.info(s"${dataShards.size} data shards initiated!")

  var numShardsFinished:Int = 0
  var fileSource:ActorRef = _

  override def receive: Receive = {

    case Master.InitParameters => {
      parametersActor ! Parameters.Init
    }
    case Master.Start => {
      log.info("Starting reading the file")
      fileSource ! Master.Ack //start reading file
      for(i <- 1 until numberOfShards)  dataShardActors ! DataShard.ReadyToProcess
    }
    case Master.Done(id) => {
      numShardsFinished += 1

      log.info(s" ${numShardsFinished} shards finished of ${dataShards.size}")

      if (numShardsFinished == dataShards.size) {
        log.info(s"JobDone")
        context.parent ! Master.JobDone
      }
    }
    case Master.Init => {println(" == Master.Init == "); fileSource  = sender}
    case Master.Ack => println(" == Master.Ack == ")
    case Master.Complete=> {println(" == Master.Complete == ")}
    case Master.Failure => println(" == Master.Failure == ")
    case dataPoint: FFMRow => {
      dataShardActors.forward(dataPoint)
      sender ! Master.Ack
    }
    case Status.Failure => println("failed")
  }
}


object DataShard {

  case object ReadyToProcess

  case object FetchParameters

}


class DataShard(  activation: Types.ActivationFunction
                , activationDerivative: Types.ActivationFunction
                , parametersActor: ActorRef
                , k:Int
                , lossTracker:ActorRef
               ) extends Actor with ActorLogging {
  implicit val timeout = Timeout(15.second)

  def receive = {
    case dataPoint:FFMRow => {
      var i = 0
      //println("actor processing datapoint")
        //parametersActor ! Parameters.UpdateCounter
        parametersActor ! Parameters.RequestParameters
        //parametersActor ! Parameters.RequestWeight(1,1)
        //parametersActor ! Parameters.UpdateWeight(1,1,0.1)
        //val x = List[(Int, Int)]((1,1),(2,1))
        /*
        println("ask")
        val future = ask(parametersActor, Parameters.RequestWeights(fj_for_ffrow(dataPoint)))
        println("reply")
        val current_weights = Await.result(future, timeout.duration).asInstanceOf[SliceVector[(Int,Int), Double]]
        */
        val r:Double = 1.0/sum(dataPoint.features.map((node:FFMNode) => node.v*node.v)) //r is ok!
        val fj:List[(Int,Int)] = fj_for_ffrow(dataPoint)
        val future_weights = ask(parametersActor, Parameters.RequestWeights(fj)).mapTo[Array[SliceVector[(Int,Int), Double]]]
        val current_weights = Await.result(future_weights, timeout.duration) //.asInstanceOf[SliceVector[(Int,Int), Double]]
        //println("=================> no update")
        val t:Double = wTx(fj, dataPoint, current_weights, r=r, k=k, do_update = false)
        //println(f"t = $t%1.3f")
        val y:Double = dataPoint.label
        val expnyt:Double = exp(-y * t)
//println(s"y=${y} | t=${t}")
        val local_loss:Double = ln(1.0+expnyt)
//println(s"ln(1+expnyt) = ${ln(1+expnyt)}")
        val kappa = -y * expnyt / (1.0+expnyt)
        //println(f"kappa = $kappa%1.3f = - $y%1.1f * $expnyt%1.3f / (1.0 + $expnyt%1.3f)")

        val future_gradient = ask(parametersActor, Parameters.RequestGradient(fj)).mapTo[Array[SliceVector[(Int,Int), Double]]]
        val current_gradient = Await.result(future_gradient, timeout.duration) //.asInstanceOf[SliceVector[(Int,Int), Double]]
        lossTracker ! Loss.UpdateLoss(local_loss)

      /*
        // gradient should have k rows
        println("current_gradient:")
        current_gradient.foreach(x => {x.foreach(y=>print(f"$y%1.3f "));print("\n")})
        print("\n")
        //
        println("current_weights:")
        current_weights.foreach(x => {x.foreach(y=>print(f"$y%1.3f "));print("\n")})
        print("\n")
        //l
        println("=================> update")
        */
        wTx(fj, dataPoint, current_weights, current_gradient, kappa = kappa, k=k, do_update = true)

      /*
        println("current_weights:")
        current_weights.foreach(x (x) => {/*do comething with requested parameter*/}
=> {x.foreach(y=>print(f"$y%1.3f" + " "));print("\n")})
        println("wtx")
*/
        i += 1
        //println(i.toString)
        //println(f"tr_loss = $tr_loss%1.10f")

      //If we have processed all of them then we are done.
      //shardManager ! ShardManager.Available(shardId)
      //context.parent ! Master.ShardRequest(shardId)
    }

  }


  def fj_for_ffrow(row:FFMRow): List[(Int,Int)] = {
    val indexes = new ListBuffer[(Int,Int)]
    val i_max = row.features.size
    var i = 0
    while(i < i_max) {
      var j = i + 1
      val node1: FFMNode = row.features(i)
      while (j < i_max) {
        val node2: FFMNode = row.features(j)
        indexes += Tuple2(node2.f, node1.j)
        indexes += Tuple2(node1.f, node2.j)
        j += 1
      }
      i += 1
    }
    //print("\n")
    //println("fj_for_ffrow: " + indexes.toString)
    indexes.toList
  }


  def wTx(fj:List[(Int,Int)], row:FFMRow, w:Array[SliceVector[(Int,Int),Double]], wg:Array[SliceVector[(Int,Int), Double]] = null, k:Int, r:Double = 1.0, kappa:Double = 0.0, eta:Double =0.1, lambda:Double = 0.00002, do_update:Boolean = false): Double = {
    var t:Double = 0.0
    //println("\nwTX | weights:")
    var w_ind:Int =0
    val i_max = row.features.size
    var i: Int = 0
    while (i < i_max) {
      var j = i + 1
      while (j < i_max) {
        val node1: FFMNode = row.features(i)
        val node2: FFMNode = row.features(j)
        //var w1: Double = w(fj.indexOf((node2.f, node1.j)))
        //var w2: Double = w(fj.indexOf((node1.f, node2.j)))
        val v = node1.v * node2.v * r

        if(do_update) {
          var d = 0
          while(d < k ) {
            val w1: Double = w(d)(w_ind)
            val w2: Double = w(d)(w_ind + 1)

            //println(f"g1=$lambda%1.3f * $w1%1.3f + $kappa%1.3f * $v%1.3f * $w2%1.3f")
            val g1 = lambda * w1 + kappa * v * w2
            val g2 = lambda * w2 + kappa * v * w1

            wg(d)(w_ind) = wg(d)(w_ind)+ g1 * g1
            wg(d)(w_ind + 1) = wg(d)(w_ind + 1) + g2 * g2

            w(d)(w_ind) = w(d)(w_ind) - eta * g1 / sqrt(wg(d)(w_ind))
            w(d)(w_ind + 1) = w(d)(w_ind + 1) - eta * g2 / sqrt(wg(d)(w_ind + 1))
          }
          w_ind = w_ind + 2
          d = d+1
        } else {
          var d = 0
          while(d < k ) {
            //println(s"${i}:${j} :: w(${d})(${w_ind}): ${w(d)(w_ind)} | w(${d})(${w_ind+1}) ${w(d)(w_ind+1)} | v: ${v} | = ${w(d)(w_ind)*w(d)(w_ind+1)*v}")
            t = t + w(d)(w_ind)*w(d)(w_ind+1)*v // w1 * w2 * xi * xj * kappa / ||x||
            //t + w1:*w2:*v //should it be sum{over d\in1..k)(w1*w2*v)?
            d = d +1
          }
        } // do_update
        j = j + 1
      } // j < i_max
      i = i + 1
    } // i < i_max

    return t // for update t=0
  }

/*
  def waitForParameters: Receive = {
    case Parameters.LatestParameters(weights) => {
    println("waitForParameters")
    latestWeights = weights
    context.parent ! DataShard.DoneFetchingParameters
    context.unbecome()
  }
  }*/

}

case class FFMRow(@transient label:Double, @transient features:BV[FFMNode])

case class FFMNode(f:Int, j:Int, var v:Double) {
  var VU:Double = 0.0
  var P:BV[Double] = null
  var DPU:Double =  0.0
  var DP:BV[Double] = null
}

object Parameters {

  case object RequestParameters

  case class UpdateWeight(i:Int,j:Int,v:Double)

  case class RequestWeights(x:List[(Int,Int)])

  case class FetchWeights(x:SliceVector[(Int,Int),Double])

  case class RequestGradient(x:List[(Int,Int)])

  case object Init
}


class Parameters(   eta: Double
                  , lambda: Double
                  , l:Int
                  , n:Int
                  , m:Int
                  , k:Int
                  , weightsInitDist: ContinuousDistr[Double]
                  , epochs:Int
                  ) extends Actor with ActorLogging {
  var weights: Array[BDM[Double]] = _
  var gradient: Array[BDM[Double]] = _


  def receive = {

    //case Parameters.RequestWeight(i,j) => {sender ! Parameters.FetchWeight(weights(i,j))}

    //case Parameters.UpdateWeight(i,j,v) => {weights(i,j) += v}

    case Parameters.RequestWeights(x) => {//println("requested weights");println(x);
       sender ! weights.map(_(x))}

    case Parameters.RequestGradient(x) => {//println("requested gradient");println(x);
       sender ! gradient.map(_(x))}

    case Parameters.Init => {
        log.info("Initializing Parameters: weights arrays")
        weights = Array.fill[BDM[Double]](k)(new BDM[Double](m, n, Array.fill(n * m)(weightsInitDist.draw)))
        // gradient is of size n*m*k
        log.info("Initializing Parameters: gradient arrays")
        gradient = Array.fill[BDM[Double]](k)(new BDM[Double](m, n, Array.fill(n * m)(1.0)))
        sender ! Master.Start
    }
  }
}


object Loss {

  case class UpdateLoss(x:Double)

  case object PrintLoss

  case object PrintNormalizedLoss

  case object PrintCount
}


class Loss(lines:Int) extends Actor with ActorLogging {
  var loss:Double = 0.0
  var count:Int = 0
  val print_every:Int = 10000
  //add elapsed time
  def receive = {
    case Loss.UpdateLoss(x:Double) => {
      count = count + 1
      loss = loss + x
      if(count%print_every==0 || count == 1) {
        val nloss: Double = loss / count
        println(f"Normalized Loss: ${nloss}%1.6f | Percentage: ${1.0*count/lines}%1.4f | Count: ${count} ")
      }
    }
    case Loss.PrintLoss => {println(s"Loss: ${loss}")}
    case Loss.PrintCount => {println(s"Count: ${count}")}
    case Loss.PrintNormalizedLoss => {val nloss:Double = loss/count; println(s"Normalized Loss: ${nloss}")}
  }
}
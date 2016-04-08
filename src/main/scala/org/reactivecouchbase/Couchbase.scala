package org.reactivecouchbase

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.actor.{ActorSystem, Scheduler}
import com.couchbase.client.java.CouchbaseAsyncCluster
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.reactivecouchbase.client._
import org.reactivecouchbase.observables._

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/**
  *
  * Representation of a Couchbase Bucket
  *
  * @param cbDriver ReactiveCouchbase driver associated with this bucket
  * @param cluster actual Java driver
  * @param bucketName name of the bucket
  * @param pass password for the bucket
  * @param timeout connection timeout
  */
class CouchbaseBucket(private[reactivecouchbase] val cbDriver: ReactiveCouchbaseDriver,
                      private[reactivecouchbase] val cluster: CouchbaseAsyncCluster,
                      val bucketName: String,
                      val pass: String,
                      val timeout: Long) extends BucketAPI {

  lazy val client =
    Await.result(cluster.openBucket(bucketName, pass).toFuture, Duration(10, TimeUnit.SECONDS))

  def logger = cbDriver.logger

  /**
    *
    * Disconnect the current Java Driver from the Couchbase server
    *
    * @return the disconnected CouchbaseBucket
    */
  def disconnect() =
    Await.result(client.close().toFuture, Duration(10, TimeUnit.SECONDS))

  /**
    * @return the actual ReactiveCouchbaseDriver
    */
  def driver: ReactiveCouchbaseDriver = cbDriver

  private[reactivecouchbase] val blockInFutures = cbDriver.configuration.getBoolean("couchbase.driver.blockinfutures").getOrElse(false)
  private[reactivecouchbase] val block = cbDriver.configuration.getBoolean("couchbase.driver.block").getOrElse(false)
  private[reactivecouchbase] val enableOperationTimeout = cbDriver.configuration.getBoolean("couchbase.driver.enableoperationtimeout").getOrElse(false)
  private[reactivecouchbase] val doubleCheck = cbDriver.configuration.getBoolean("couchbase.driver.doublecheck").getOrElse(false)

  /**
    * Check if Futures from Java Driver are failed. If so, fails scala Future
    */
  private[reactivecouchbase] val checkFutures = cbDriver.configuration.getBoolean("couchbase.driver.checkfuture").getOrElse(false)

  /**
    * Fails Json processing if Json structure is wrong
    */
  private[reactivecouchbase] val jsonStrictValidation = cbDriver.configuration.getBoolean("couchbase.json.validate").getOrElse(true)

  /**
    * Fail Scala futures if OperationStatus is failed
    */
  private[reactivecouchbase] val failWithOpStatus = cbDriver.configuration.getBoolean("couchbase.failfutures").getOrElse(false)

  private[reactivecouchbase] val failWithNonStringDoc = cbDriver.configuration.getBoolean("couchbase.failonnonstring").getOrElse(false)

  /**
    * Timeout
    */
  private[reactivecouchbase] val ecTimeout: Long = cbDriver.configuration.getLong("couchbase.akka.timeout").getOrElse(60000L)

  /**
    * Optional N1QL host
    */
  private[reactivecouchbase] val N1QLHost = driver.configuration.getString("couchbase.n1ql.host")

  /**
    * Optional N1QL port
    */
  private[reactivecouchbase] val N1QLPort = driver.configuration.getString("couchbase.n1ql.port")

  /**
    * Configuration for HTTP client
    */
  private[reactivecouchbase] val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
                                                                 .setAllowPoolingConnections(cbDriver.configuration.getBoolean("couchbase.http.pool").getOrElse(true))
                                                                 .setCompressionEnforced(cbDriver.configuration.getBoolean("couchbase.http.compression").getOrElse(true))
                                                                 .setRequestTimeout(cbDriver.configuration.getInt("couchbase.http.reqtimeout").getOrElse(6))
                                                                 .setPooledConnectionIdleTimeout(cbDriver.configuration.getInt("couchbase.http.idlepool").getOrElse(6))
                                                                 .setConnectTimeout(cbDriver.configuration.getInt("couchbase.http.idleconnection").getOrElse(6))
                                                                 .setMaxConnections(cbDriver.configuration.getInt("couchbase.http.maxTotalConnections").getOrElse(-1))
                                                                 .setMaxConnectionsPerHost(cbDriver.configuration.getInt("couchbase.http.maxConnectionsPerHost").getOrElse(-1))
                                                                 .build()

  /**
    * The HTTP client dedicated for this bucket (used for view queries and N1QL support)
    */
  private[reactivecouchbase] val httpClient: AsyncHttpClient = new AsyncHttpClient(config)
}

/**
  * Driver to access a Couchbase server
  *
  *
  * Example :
  * {{{
  *   val driver = ReactiveCouchbaseDriver()
  *   val bucket = driver.bucket("default")
  *   val capped = driver.cappedBucket("default", 100, true)
  * }}}
  */
class ReactiveCouchbaseDriver(as: ActorSystem, config: Configuration, log: LoggerLike, val mode: Mode) {

  val hosts = config.getStringList("couchbase.hosts")
              .getOrElse(throw new ReactiveCouchbaseException("No configuration", "Can't find any hosts in conf !!"))

  // TODO implement more configuration settings
  /*if (cbDriver.configuration.getBoolean("couchbase.driver.useec").getOrElse(true)) {
      cfb.setListenerExecutorService(ExecutionContextExecutorServiceBridge.apply(cbDriver.executor()))
    }

    cbDriver.configuration.getLong("couchbase.driver.native.authWaitTime").foreach(cfb.setAuthWaitTime)
    cbDriver.configuration.getLong("couchbase.driver.native.obsPollInterval").foreach(cfb.setObsPollInterval)
    cbDriver.configuration.getLong("couchbase.driver.native.obsTimeout").foreach(cfb.setObsTimeout)
    cbDriver.configuration.getLong("couchbase.driver.native.reconnectThresholdTime").foreach(cfb.setReconnectThresholdTime(_, TimeUnit.MILLISECONDS))
    cbDriver.configuration.getInt("couchbase.driver.native.viewConnsPerNode").foreach(cfb.setViewConnsPerNode)
    cbDriver.configuration.getInt("couchbase.driver.native.viewTimeout").foreach(cfb.setViewTimeout)
    cbDriver.configuration.getInt("couchbase.driver.native.viewWorkerSize").foreach(cfb.setViewWorkerSize)
    cbDriver.configuration.getBoolean("couchbase.driver.native.useNagleAlgorithm").foreach(cfb.setUseNagleAlgorithm)
    cbDriver.configuration.getBoolean("couchbase.driver.native.daemon").foreach(cfb.setDaemon)
    cbDriver.configuration.getInt("couchbase.driver.native.timeoutExceptionThreshold").foreach(cfb.setTimeoutExceptionThreshold)
    cbDriver.configuration.getBoolean("couchbase.driver.native.shouldOptimize").foreach(cfb.setShouldOptimize)
    cbDriver.configuration.getLong("couchbase.driver.native.maxReconnectDelay").foreach(cfb.setMaxReconnectDelay)
    cbDriver.configuration.getLong("couchbase.driver.native.opQueueMaxBlockTime").foreach(cfb.setOpQueueMaxBlockTime)
    cbDriver.configuration.getLong("couchbase.driver.native.opTimeout").foreach(cfb.setOpTimeout)
    cbDriver.configuration.getLong("couchbase.driver.native.maxReconnectDelay").foreach(cfb.setMaxReconnectDelay)
    cbDriver.configuration.getInt("couchbase.driver.native.readBufferSize").foreach(cfb.setReadBufferSize)

    // Sets the metric Collector, ths class must extent AbstractMetricCollector
    cbDriver.configuration.getString("couchbase.driver.native.setMetricCollector").foreach(x => {
      cfb.setMetricCollector(Class.forName(x).newInstance().asInstanceOf[AbstractMetricCollector])
    })

    // SInce MetricType is an enum it iteratetes through possible values and sets the correct MetricType
    MetricType.values().find(prop => prop.toString.equalsIgnoreCase(cbDriver.configuration.getString("couchbase.driver.native.enableMetrics").getOrElse(MetricType.OFF.name()))).foreach(cfb.setEnableMetrics)*/


  private lazy val environment =
    DefaultCouchbaseEnvironment.builder().build()

  private lazy val cluster = CouchbaseAsyncCluster.create(environment, hosts)

  /**
    * All the buckets managed by this driver
    */
  val buckets = new ConcurrentHashMap[String, CouchbaseBucket]

  /**
    * Config for each bucket
    */
  val bucketsConfig: Map[String, Config] = config.getObjectList("couchbase.buckets")
                                           .getOrElse(throw new ReactiveCouchbaseException("No configuration", "Can't find any bucket in conf !!!"))
                                           .map(c => new Configuration(c.toConfig))
                                           .map(b => (b.getString("alias").getOrElse(b.getString("bucket").getOrElse(throw new ReactiveCouchbaseException("Error", "No bucket name :("))), b.underlying))
                                           .toMap

  /**
    * @return actor system for this driver
    */
  def system(): ActorSystem = as

  /**
    * @return ExecutionContext for this driver
    */
  def executor(): ExecutionContext = as.dispatcher

  /**
    * @return Akka Scheduler for this driver
    */
  def scheduler(): Scheduler = as.scheduler

  /**
    * @return Configuration for this driver
    */
  def configuration: Configuration = config

  /**
    * @return Logger wrapper for this driver
    */
  def logger: LoggerLike = log

  def bucket(bucket: String, pass: String, timeout: Long): CouchbaseBucket = {
    if (!buckets.containsKey(bucket)) {
      buckets.putIfAbsent(bucket, new CouchbaseBucket(this, cluster, bucket, pass, timeout))
    }

    buckets.get(bucket)
  }

  def bucket(name: String): CouchbaseBucket = {
    if (!buckets.containsKey(name)) {
      val cfg = new Configuration(bucketsConfig.getOrElse(name, throw new ReactiveCouchbaseException("No bucket", s"Cannot find bucket $name")))
      val buck: String = cfg.getString("bucket").getOrElse("default")
      val pass: String = cfg.getString("pass").getOrElse("")
      val timeout: Long = cfg.getLong("timeout").getOrElse(0)
      bucket(buck, pass, timeout)
    } else {
      buckets.get(name)
    }
  }

//  /**
//    *
//    * Provide a Bucket as Capped bucket.
//    * A capped bucket is a special bucket where insertion order of documents is kept
//    *
//    * @param name name of the Couchbase bucket
//    * @param max max number of element in the capped bucket
//    * @param reaper activate reaper to delete elements when the capped bucket is full (ie. bigger than max)
//    * @return a capped bucket
//    */
//  def cappedBucket(name: String, max: Int, reaper: Boolean): CappedBucket = CappedBucket(() => bucket(name), bucket(name).driver.executor(), max, reaper)

//  /**
//    *
//    * Provide a Bucket as Capped bucket.
//    * A capped bucket is a special bucket where insertion order of documents is kept
//    *
//    * @param name name of the Couchbase bucket
//    * @param ec the ExecutionContext for async processing
//    * @param max max number of element in the capped bucket
//    * @param reaper activate reaper to delete elements when the capped bucket is full (ie. bigger than max)
//    * @return a capped bucket
//    */
//  def cappedBucket(name: String, ec: ExecutionContext, max: Int, reaper: Boolean): CappedBucket = CappedBucket(() => bucket(name), ec, max, reaper)

  /**
    *
    * Create a N1QL query
    *
    * @param query the actual query written in N1QL query language
    * @return a new N1QL query
    */
  def N1QL(query: String): N1QLQuery = {
    CouchbaseN1QL.N1QL(query)(this.buckets.head._2)
  }

  /**
    * Shutdown this driver
    */
  def shutdown() {
    buckets.foreach(t => {
      t._2.disconnect()
      t._2.httpClient.close()
    })
//    CappedBucket.clearCache()
    system().terminate()
  }
}

/**
  * Companion object to build ReactiveCouchbaseDriver
  */
object ReactiveCouchbaseDriver {

  /**
    *
    * Create the default ActorSystem if not provided
    *
    * @return the default actor system
    */
  private def defaultSystem = {
    import com.typesafe.config.ConfigFactory
    val config = ConfigFactory.load()
    ActorSystem("ReactiveCouchbaseSystem", config.getConfig("couchbase.akka"))
  }

  /**
    * @return a new ReactiveCouchbaseDriver with default actor system and configuration
    */
  def apply(): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(defaultSystem, new Configuration(ConfigFactory.load()), StandaloneLogger.configure(), Prod())

  /**
    * @param system a custom ActorSystem provided by the user
    * @return a new ReactiveCouchbaseDriver with default configuration
    */
  def apply(system: ActorSystem): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, new Configuration(ConfigFactory.load()), StandaloneLogger.configure(), Prod())

  /**
    * @param system a custom ActorSystem provided by the user
    * @param config a custom configuration provided by the user
    * @return a new ReactiveCouchbaseDriver
    */
  def apply(system: ActorSystem, config: Configuration): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, StandaloneLogger.configure(), Prod())

  /**
    * @param system a custom ActorSystem provided by the user
    * @param config a custom configuration provided by the user
    * @param logger a custom logger wrapper provided by the user
    * @return a new ReactiveCouchbaseDriver
    */
  def apply(system: ActorSystem, config: Configuration, logger: LoggerLike): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, logger, Prod())

  /**
    * @param system a custom ActorSystem provided by the user
    * @param config a custom configuration provided by the user
    * @param logger a custom logger wrapper provided by the user
    * @param mode mode of the driver
    * @return a new ReactiveCouchbaseDriver
    */
  def apply(
             system: ActorSystem = defaultSystem,
             config: Configuration = new Configuration(ConfigFactory.load()),
             logger: LoggerLike = StandaloneLogger.configure(),
             mode: Mode = Prod()
           ): ReactiveCouchbaseDriver = new ReactiveCouchbaseDriver(system, config, logger, mode)
}

/**
  * Main API for Couchbase generic operations.
  */
object Couchbase extends Read with Write with Delete with Counters with Queries /*with JavaApi*/ with Atomic {}

trait Mode {
  def name: String
}

case class Dev() extends Mode {
  def name: String = "dev_"
}

case class Prod() extends Mode {
  def name: String = ""
}

case class Test() extends Mode {
  def name: String = ""
}
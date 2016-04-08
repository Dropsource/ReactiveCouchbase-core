import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.reactivecouchbase.{Configuration, ReactiveCouchbaseDriver}

val config = ConfigFactory.parseString(
  """
    |couchbase {
    |  host = ["127.0.0.1:8091"]
    |  akka {
    |    timeout=20000
    |    execution-context {
    |      fork-join-executor {
    |        parallelism-factor = 4.0
    |        parallelism-max = 40
    |      }
    |    }
    |  }
    |  failfutures=false
    |  json {
    |    validate=true
    |  }
    |  buckets = [
    |    {
    |      bucket = "default"
    |      user = "admin"
    |      pass = "queuefree"
    |      timeout = "0"
    |    }
    |  ]
    |  driver {
    |    useec=true
    |    blockinfutures=false
    |    block=false
    |    enableoperationtimeout=false
    |  }
    |}
  """.stripMargin)

val akkas = config.getObject("couchbase.akka").toConfig
val as = ActorSystem("ReactiveCouchbaseSystem", akkas)
val driver = ReactiveCouchbaseDriver(as, new Configuration(config))


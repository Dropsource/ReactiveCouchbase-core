import org.reactivecouchbase.CouchbaseRWImplicits.jsObjectToDocumentWriter
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable.Specification
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Await

class AppendSpec extends Specification {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucketDefault = driver.bucket("default")

  "ReactiveCouchbase append API" should {

    "insert" in {
      Await.result(bucketDefault.set[JsObject]("alias-key", Json.obj("hello" -> "world")), timeout)
      success
    }

    "append shit" in {
      //val id = bucketDefault.couchbaseClient.gets("alias-key").getCas
      bucketDefault.couchbaseClient.append("alias-key",""" "added":"stuff" """).get()
      println("\n\n\n" + bucketDefault.couchbaseClient.get("alias-key"))
      success
    }

   /* "delete from default bucket" in {
      Await.result(bucketDefault.delete("alias-key"), timeout)
      success
    }*/

    "shutdown now" in {
      driver.shutdown()
      success
    }
  }
}
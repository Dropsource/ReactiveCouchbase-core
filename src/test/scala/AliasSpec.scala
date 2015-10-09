import org.reactivecouchbase.CouchbaseRWImplicits.{documentAsJsObjectReader, jsObjectToDocumentWriter}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.specs2.mutable.Specification
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Await

class AliasSpec extends Specification {
  sequential

  import Utils._

  """
You need to start a Couchbase server with a 'default' bucket on standard port to run those tests ...
  """ in ok

  val driver = ReactiveCouchbaseDriver()
  val bucketDefault = driver.bucket("default")
  val aliasedBucketDefault = driver.bucket("aliased-default")

  "ReactiveCouchbase aliases" should {

    "insert data in default bucket" in {
      Await.result(bucketDefault.set[JsObject]("alias-key", Json.obj("hello" -> "world")), timeout)
      success
    }

    "fetch data from aliased default bucket" in {
      Await.result(aliasedBucketDefault.get[JsObject]("alias-key").map { json =>
        json mustEqual Some(Json.obj("hello" -> "world"))
      }, timeout)
      success
    }

    "delete from default bucket" in {
      Await.result(bucketDefault.delete("alias-key"), timeout)
      success
    }

    "shutdown now" in {
      driver.shutdown()
      driver.shutdown()
      success
    }
  }
}
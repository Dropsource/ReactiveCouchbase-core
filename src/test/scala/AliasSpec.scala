import com.couchbase.client.java.document.json.JsonObject
import org.reactivecouchbase.CouchbaseRWImplicits.{documentAsJsObjectReader, jsObjectToDocumentWriter}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.specs2.mutable.Specification
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Await

class AliasSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import Utils._

  val driver = ReactiveCouchbaseDriver()
  val bucketDefault = driver.bucket("default")
  val aliasedBucketDefault = driver.bucket("aliased-default")

  "ReactiveCouchbase aliases" should {

    "insert data in default bucket" in {
      Await.result(bucketDefault.set[JsObject, JsonObject]("alias-key", Json.obj("hello" -> "world")), timeout)
    }

    "fetch data from aliased default bucket" in {
      Await.result(aliasedBucketDefault.get[JsObject]("alias-key").map { json =>
        json mustEqual Some(Json.obj("hello" -> "world"))
      }, timeout)
    }

    "delete from default bucket" in {
      Await.result(bucketDefault.delete("alias-key"), timeout)
    }

    "shutdown now" in {
      driver.shutdown()
    }
  }
}
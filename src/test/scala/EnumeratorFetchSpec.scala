import org.reactivecouchbase.CouchbaseRWImplicits.documentAsJsObjectReader
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.specs2.mutable.Specification
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.JsObject

import scala.concurrent.Await

class EnumeratorFetchSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import Utils._

  val driver = ReactiveCouchbaseDriver()
  val bucketDefault = driver.bucket("default")

  override protected def afterAll(): Unit = {
    driver.shutdown()
  }

  "ReactiveCouchbase Read API" should {

    "not be able to fetch non existing values" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Seq[String]("aspoQdxjSUFKiJ5Vzes56Serpjbv78Xz0SDBdbataH26kOJ8I4spAjrJoLmk2Cca", "E46T7OpyQM8c2du63L0GYtNeyA8e1rRMG9WNynEliCMrHZgQOgT4HEgEzvJWynQo")).toList, timeout)
      list.size mustEqual 0
    }

    "be able to fetch no values" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Seq[String]()).toList, timeout)
      list.size mustEqual 0
    }

    "be able to fetch no values from enumerator" in {
      val list = Await.result(bucketDefault.fetchValues[JsObject](Enumerator.empty[String]).toList, timeout)
      list.size mustEqual 0
    }
  }
}
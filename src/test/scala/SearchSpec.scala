import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view.{DefaultView, DesignDocument, Stale, ViewQuery}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import scala.collection.JavaConversions._

import scala.concurrent._
import scala.util.Try

class SearchSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import Utils._

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  override protected def beforeAll(): Unit = {

    // Clear out DB
    Try(Await.result(bucket.flush(), timeout))
    Try(Await.result(bucket.deleteDesignDoc("persons"), timeout))


    // Create design doc
    val designDoc = DesignDocument.create("persons", List(
      DefaultView.create("by_name", "function (doc, meta) { emit(doc.name, null); }"),
      DefaultView.create("by_surname", "function (doc, meta) { emit(doc.surname, null); }"),
      DefaultView.create("by_age", "function (doc, meta) { emit(doc.age, null); }")
    ))

    Await.result(bucket.createDesignDoc(designDoc), timeout)

    // Insert seed data
    for (i <- 0 to 99) {
      Await.result(bucket.add[Person, JsonObject](s"person--$i", Person("Billy", s"Doe-$i", i)), timeout)
    }
  }

  override protected def afterAll(): Unit = {
    Await.result(bucket.deleteDesignDoc("persons"), timeout)
    for (i <- 0 to 99) {
      Await.result(bucket.delete(s"person--$i"), timeout)
    }

    driver.shutdown()
  }


  "ReactiveCouchbase search API" should {

    "find people younger than 42" in {
      val query = ViewQuery.from("persons", "by_age").includeDocs(true).stale(Stale.FALSE).startKey(0).endKey(41)
      val res = Await.result(bucket.searchValues[Person](query).toList.map { list =>
        list.map { person =>
          if (person.age >= 42)
            assert(false, s"$person is older than 42")

          person
        }
      }, timeout)

      res.size mustEqual 42
    }

    "find people older than 42" in {
      val query = ViewQuery.from("persons", "by_age").includeDocs(true).stale(Stale.FALSE).startKey(43).endKey(100)
      Await.result(bucket.searchValues[Person](query).toList.map { list =>
        list.foreach { person =>
          if (person.age <= 42)
            assert(false, s"$person is younger than 42")
        }
      }, timeout)

      assert(true)
    }

    "find people named Billy" in {
      val query = ViewQuery.from("persons", "by_name").includeDocs(true).stale(Stale.FALSE).startKey("Billy").endKey("Billy\uefff")

      Await.result(bucket.searchValues[Person](query).toList.map { list =>
        if (list.size != 100) assert(false, s"No enought persons : ${list.size}")
      }, timeout)

      assert(true)
    }

    "find people named Bobby" in {
      val query = ViewQuery.from("persons", "by_name").includeDocs(true).stale(Stale.FALSE).startKey("Bobby").endKey("Bobby\uefff")

      Await.result(bucket.searchValues[Person](query).toList.map { list =>
        if (list.nonEmpty) assert(false, "Found people named Bobby")
      }, timeout)

      assert(true)
    }
  }
}

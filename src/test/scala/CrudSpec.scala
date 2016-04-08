import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.json.JsonObject
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

class CrudSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import Utils._

  val duration = Duration(10, TimeUnit.SECONDS)

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  override protected def beforeAll(): Unit = {
    Await.result(bucket.set[Person, JsonObject]("person-key1", Person("John", "Doe", 42)), duration)
    Await.result(bucket.set[Person, JsonObject]("person-key2", Person("Jane", "Doe", 42)), duration)
    Await.result(bucket.set[Person, JsonObject]("person-key3", Person("Billy", "Doe", 42)), duration)
  }


  override protected def afterAll(): Unit = {
    Await.result(bucket.delete("person-key1"), duration)
    Await.result(bucket.delete("person-key2"), duration)
    Await.result(bucket.delete("person-key3"), duration)
    driver.shutdown()
  }

  "ReactiveCouchbase and its BulkAPI" should {

    "not be able to find some data" in {
      bucket.get[Person]("person-does-not-exist") foreach { p =>
        assert(p.isEmpty)
      }
    }

    "be able to find some data" in {
      bucket.get[Person]("person-key1") foreach { p =>
        assert(p.isDefined)
      }
    }

    "fetch some data" in {
      val expectedPersons = List(
        Person("John", "Doe", 42),
        Person("Jane", "Doe", 42),
        Person("Billy", "Doe", 42)
      )
      var seq = List[Future[Seq[Option[Person]]]]()
      for (i <- 0 to 100) {
        val fut: Future[List[Option[Person]]] = Future.sequence(List(
          bucket.get[Person]("person-key1"),
          bucket.get[Person]("person-key2"),
          bucket.get[Person]("person-key3")
        )).map { list =>
          //println("Got something : " + list)
          if (list == null || list.isEmpty) assert(false, "List should not be empty ...")
          list.foreach { opt =>
            if (opt.isEmpty) {
              assert(false, "Cannot fetch John Doe")
            }
            val person = opt.get
            expectedPersons.contains(person).mustEqual(true)
          }
          list
        }

        seq = seq :+ fut
      }
    }

    "update some data" in {
      bucket.replace[Person, JsonObject]("person-key1", Person("John", "Doe", 43)) foreach { updated =>
        bucket.get[Person]("person-key1") foreach {
          p =>
          assert(p.isDefined && p.get.age == 43)
        }
      }
    }


    "delete some data" in {
      Await.result(bucket.set[Person, JsonObject]("person-test", Person("John", "Doe", 42)), duration)
      assert(Try(Await.result(bucket.delete("person-test"), duration)).isSuccess)
    }

  }
}
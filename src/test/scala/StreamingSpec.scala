import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator

import scala.concurrent._

class StreamingSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  import Utils._

  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")
  val personsAndKeys = List(
    ("person-1", Person("John", "Doe", 42)),
    ("person-2", Person("Jane", "Doe", 42)),
    ("person-3", Person("Billy", "Doe", 42)),
    ("person-4", Person("Bobby", "Doe", 42))
  )

  val persons = List(
    Person("John", "Doe", 42) ,
    Person("Jane", "Doe", 42) ,
    Person("Billy", "Doe", 42),
    Person("Bobby", "Doe", 42)
  )
  val keys = List(
    "person-1",
    "person-2",
    "person-3",
    "person-4"
  )

  "ReactiveCouchbase streaming API" should {

    "insert streamed data" in {
      Await.result(bucket.setStream(Enumerator.enumerate(personsAndKeys)).map { results =>
        results.foreach(r => if (!r.isSuccess) failure(s"Can't persist element : ${r.getMessage}"))
      }, timeout)
      success
    }

    "fetch some data" in {
      keys.foreach { key =>
        val fut = bucket.get[Person](key).map { opt =>
          if (opt.isEmpty) {
            failure("Cannot fetch element")
          }
          val person = opt.get
          persons.contains(person).mustEqual(true)
        }
        Await.result(fut, timeout)
      }
      success
    }

    "add streamed data" in {
      Await.result(bucket.addStream(Enumerator.enumerate(personsAndKeys)).map { results =>
        results.foreach(r => if (r.isSuccess) failure(s"Can persist element : ${r.getMessage}"))
      }, timeout)
      success
    }

    "delete some data" in {
      Await.result(bucket.deleteStream(Enumerator.enumerate(keys)).map { results =>
        results.foreach(r => if (!r.isSuccess) failure(s"Can't delete element : ${r.getMessage}"))
      }, timeout)
      success
    }

    "add streamed data (again)" in {
      Await.result(bucket.addStream(Enumerator.enumerate(personsAndKeys)).map { results =>
        results.foreach(r => if (!r.isSuccess) failure(s"Can't persist element : ${r.getMessage}"))
      }, timeout)
      success
    }

    "delete some data (again)" in {
      Await.result(bucket.deleteStream(Enumerator.enumerate(keys)).map { results =>
        results.foreach(r => if (!r.isSuccess) failure(s"Can't delete element : ${r.getMessage}"))
      }, timeout)
      success
    }

    "shutdown now" in {
      //Await.result(bucket.flush(), timeout)
      driver.shutdown()
      success
    }
  }
}

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.{JsonDocument, JsonLongDocument}
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.reactivecouchbase.observables._
import org.scalatest.{MustMatchers, WordSpec}
import play.api.libs.json.JsNumber
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CounterSpec extends WordSpec with MustMatchers {

  implicit val duration = Duration(5, TimeUnit.SECONDS)
  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase Counter API" should {

    "throw exception if not exists" in {
      bucket.incr("TestInc", 5L) map {
        case None => assert(true)
        case _ => assert(false, "id TestInc does not exist, should return none")
      }
    }

    "increase document that does exist" in {
      bucket.add[JsNumber, Long]("TestInc", JsNumber(1L)).await

      bucket.incr("TestInc", 5L) map {
        case Some(long) => assert(long == 6L)
        case _ => assert(false, "Invalid long value")
      }

      bucket.getLong("TestInc").await must contain(6L)

      bucket.delete("TestInc").await
    }

    /*"throw exception if not exists" in {

      Observable
      .just(1, 2, 3, 4, 5)
      .flatMap(k => toScalaObservable(bucket.client.get[JsonLongDocument](JsonLongDocument.create(k.toString))))
      .subscribe(document => println("Got: " + document))


      Observable
      .just(1, 2, 3)
      .doOnNext(i => if (i == 2) throw new RuntimeException("I don't like 2"))
      .subscribe(
        nextInt => println("Got: " + nextInt),
        err => println("Whoops: " + err.getMessage)
      )

      bucket.incr("TestInc", 5L)

      toScalaObservable(bucket.client.counter("testInc", 5L))
      .onErrorReturn(err => JsonDocument.create("FAILED"))
      .subscribe(
        next => println("Got: " + next),
        err => println("Whoops: " + err.getMessage)
      )

    }*/

    /*"be able to handle integers" in {
      Await.result(bucket.setInt("counter", 2), timeout)
      Await.result(bucket.getInt("counter"), timeout) mustEqual 2
      Await.result(bucket.delete("counter"), timeout)
      success
    }

    "be able to handle long" in {
      Await.result(bucket.setLong("counterLong", 4L), timeout)
      Await.result(bucket.getLong("counterLong"), timeout) mustEqual 4L
      Await.result(bucket.delete("counterLong"), timeout)
      success
    }*/


    /*"be able to increment longs" in {

      Await.result(bucket.add[JsNumber, JsonLongDocument]("incrementLong", JsNumber(0L)), timeout)
      Await.result(bucket.incr("incrementLong", 3L), timeout)
      Await.result(bucket.incr("incrementLong", 1L), timeout) must equal(4L)
      Await.result(bucket.delete("incrementLong"), timeout)
    }*/


    /*"be able to decrement longs" in {
      Await.result(bucket.add[JsNumber, JsonLongDocument]("decrementLong", JsNumber(0L)), timeout)
      Await.result(bucket.incr("decrementLong", 3L), timeout)
      Await.result(bucket.incr("decrementLong", -1L), timeout) must equal(2L)
      Await.result(bucket.delete("decrementLong"), timeout)

    }*/


    "shutdown now" in {
      driver.shutdown()
    }
  }
}
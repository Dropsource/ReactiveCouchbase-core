import com.couchbase.client.java.document.JsonLongDocument
import org.reactivecouchbase.ReactiveCouchbaseDriver
import org.scalatest.{MustMatchers, Matchers, WordSpec}
import org.specs2.mutable.Specification
import play.api.libs.json.JsNumber

import scala.concurrent.Await

class CounterSpec extends WordSpec with MustMatchers {

  import Utils._


  val driver = ReactiveCouchbaseDriver()
  val bucket = driver.bucket("default")

  "ReactiveCouchbase Counter API" should {

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


    "be able to increment longs" in {

    	Await.result( bucket.add[JsNumber, JsonLongDocument]("incrementLong", JsNumber(0L)), timeout )
    	Await.result( bucket.incr("incrementLong", 3L), timeout )
    	Await.result( bucket.incr("incrementLong", 1L), timeout ) must equal(4L)
    	Await.result( bucket.delete("incrementLong"), timeout )
    }
    
    /*"be able to increment integers" in {
      
    	Await.result( bucket.setInt("incrementInt",999) , timeout )	
    	Await.result( bucket.incrAndGet("incrementInt", 1), timeout) mustEqual 1000
    	Await.result( bucket.delete("incrementInt"), timeout )
    	success
      
    }*/

    "be able to decrement longs" in {
      Await.result( bucket.add[JsNumber, JsonLongDocument]("decrementLong", JsNumber(0L)), timeout )
      Await.result( bucket.incr("decrementLong", 3L), timeout )
    	Await.result( bucket.incr("decrementLong", -1L), timeout ) must equal(2L)
    	Await.result( bucket.delete("decrementLong"), timeout )

    }
    
    /*"be able to decrement integers" in {
      
    	Await.result( bucket.setInt("decrementInt",999) , timeout )	
    	Await.result( bucket.decrAndGet("decrementInt", 1), timeout) mustEqual 998
    	Await.result( bucket.delete("decrementInt"), timeout )
    	success
      
    }*/


    "shutdown now" in {
      driver.shutdown()
    }
  }
}
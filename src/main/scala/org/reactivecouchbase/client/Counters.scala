package org.reactivecouchbase.client

import com.couchbase.client.java.document.JsonLongDocument
import com.couchbase.client.java.error.DocumentDoesNotExistException
import org.reactivecouchbase.CouchbaseBucket
import org.reactivecouchbase.observables._
import play.api.libs.json.JsNumber
import rx.Observable

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait for number operations
  */
trait Counters {

  /**
    * Increment an Int
    *
    * @param key key of the Int value
    * @param by increment of the value
    * @param bucket bucket to use
    * @param ec ExecutionContext for async processing
    * @return
    */
  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
    bucket.client.counter(key, by)
    /*.onErrorReturn(new SFunc1[Throwable, JsonLongDocument]({
      case de: DocumentDoesNotExistException => null
      case e: Exception => throw e
    }))*/
    .toFuture.map {
      case l: JsonLongDocument => l.content()
    }
  }

  //  /**
  //   *
  //   * Decrement an Int
  //   *
  //   * @param key key of the Int value
  //   * @param by the value to decrement
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def decr(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[Int] = {
  //    waitForOperation( bucket.couchbaseCluster.asyncDecr(key, by: java.lang.Integer), bucket, ec ).map(_.toInt)
  //  }
  //
  //  /**
  //   *
  //   * Decrement a Long
  //   *
  //   * @param key key of the Long value
  //   * @param by the value to decrement
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def decr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext):  Future[Long] = {
  //    waitForOperation( bucket.couchbaseCluster.asyncDecr(key, by: java.lang.Long), bucket, ec ).map(_.toLong)
  //  }
  //
  //  /**
  //   *
  //   * Increment and get an Int
  //   *
  //   * @param key key of the Int value
  //   * @param by the value to increment
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def incrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
  //    incr(key, by)(bucket, ec).map(_.toInt)
  //  }
  //
  //  /**
  //   *
  //   * Increment and get a Long
  //   *
  //   * @param key key of the Long value
  //   * @param by the value to increment
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def incrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
  //    incr(key, by)(bucket, ec)
  //  }
  //
  //  /**
  //   *
  //   * Decrement and get an Int
  //   *
  //   * @param key key of the Int value
  //   * @param by the value to decrement
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def decrAndGet(key: String, by: Int)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Int] = {
  //    decr(key, by)(bucket, ec).map(_.toInt)
  //  }
  //
  //  /**
  //   *
  //   * Decrement and get a Long
  //   *
  //   * @param key key of the Long value
  //   * @param by the value to decrement
  //   * @param bucket bucket to use
  //   * @param ec ExecutionContext for async processing
  //   * @return
  //   */
  //  def decrAndGet(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Long] = {
  //    decr(key, by)(bucket, ec)
  //  }
}

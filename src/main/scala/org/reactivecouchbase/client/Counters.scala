package org.reactivecouchbase.client

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{JsonDocument, JsonLongDocument}
import org.reactivecouchbase.CouchbaseBucket
import org.reactivecouchbase.observables._
import rx.lang.scala.JavaConversions._

import scala.collection.JavaConverters._
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
  def incr(key: String, by: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[Long]] = {
    toScalaObservable(bucket.client.counter(key, by))
    .onErrorReturn(t => JsonDocument.create("__failure__", JsonObject.from(Map("error" -> t.toString).asJava)))
    .toFuture
    .map {
      case l: JsonLongDocument =>
        Some(l.content())
      case err: JsonDocument if err.id() == "__failure__" =>
        None
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

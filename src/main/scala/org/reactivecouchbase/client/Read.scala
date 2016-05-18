package org.reactivecouchbase.client

import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.document.{BinaryDocument, JsonLongDocument, StringDocument}
import org.reactivecouchbase.CouchbaseBucket
import org.reactivecouchbase.observables._
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}
import play.api.libs.json._
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait for read operations
  */
trait Read {

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @param bucket the bucket to use
    * @return
    */
  def rawFetch(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] =
    QueryEnumerator(() => keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      Observable.from(keys).flatMap(key => bucket.client.get(key)).toList.toFuture.map { results =>
        Enumerator.enumerate(results.map(j => j.id() -> j.content().toString))
      }
    })

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @param bucket the bucket to use
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetch[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] =
    QueryEnumerator(() => rawFetch(keysEnumerator)(bucket, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, String)](t => (t._1, r.reads(Json.parse(t._2)))) &> Enumeratee.collect[(String, JsResult[T])] {
        case (k: String, JsSuccess(value, _))                            => (k, value)
        case (k: String, JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toJson(errors))
      }
    })

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @param bucket the bucket to use
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] =
    QueryEnumerator(() => fetch[T](keysEnumerator)(bucket, r, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, T)](_._2)
    })

  /**
    *
    * Fetch a stream of documents
    *
    * @param keys the key of the documents
    * @param bucket the bucket to use
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetch[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] =
    fetch[T](Enumerator.enumerate(keys))(bucket, r, ec)

  /**
    *
    * Fetch a stream of documents
    *
    * @param keys the key of the documents
    * @param bucket the bucket to use
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetchValues[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    fetchValues[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

  /**
    *
    * Fetch a optional document
    *
    * @param key the key of the document
    * @param bucket the bucket to use
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def get[T](key: String)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    toScalaObservable(bucket.client.get(key)).foldLeft[Option[JsResult[T]]](None) { case (default, json) =>
      Some(r.reads(Json.parse(json.content().toString)))
    }.toFuture map {
      case Some(JsSuccess(value, _))                            => Some(value)
      case Some(JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toJson(errors))
      case None                                                 => None
    }
  }

  def getBinaryBlob(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[String]] =
    toScalaObservable(bucket.client.get(key, classOf[BinaryDocument])).foldLeft[Option[String]](None) { case (default, binary) =>
      Some(binary.content().toString(CharsetUtil.UTF_8))
    }.toFuture


  def getStringBlob(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[String]] =
    toScalaObservable(bucket.client.get(key, classOf[StringDocument])).foldLeft[Option[String]](None) { case (default, string) =>
      Some(string.content())
    }.toFuture

  def getLong(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Option[Long]] =
    toScalaObservable(bucket.client.get(key, classOf[JsonLongDocument])).foldLeft[Option[Long]](None) { case (default, long) =>
      Some(long.content())
    }.toFuture

  /**
    *
    * Fetch a stream document and their key
    *
    * @param keys the keys of the documents
    * @param bucket the bucket to use
    * @param r Json reader
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return
    */
  def fetchWithKeys[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

  /**
    *
    * Fetch a stream document and their key
    *
    * @param keys the keys of the documents
    * @param bucket the bucket to use
    * @param r Json reader
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return
    */
  def fetchWithKeys[T](keys: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Future[Map[String, T]] = {
    fetch[T](keys)(bucket, r, ec).toList(ec).map(_.toMap)
  }

}

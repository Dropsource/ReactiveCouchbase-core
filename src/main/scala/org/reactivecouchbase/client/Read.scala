package org.reactivecouchbase.client

import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.document.{JsonLongDocument, StringDocument, BinaryDocument, JsonDocument}
import org.reactivecouchbase.CouchbaseBucket
import org.reactivecouchbase.observables._
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}
import play.api.libs.json._
import rx.Observable

import scala.collection.JavaConversions._
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
  def rawFetch(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] = {
    QueryEnumerator(() => keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      Observable.from(keys).scFlatMap(key => bucket.client.get(key)).toList.toFuture.map { results =>
        Enumerator.enumerate(results.map(j => j.id() -> j.content().toString))
      }
    })
  }

  /*def rawFetch2(keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[(String, String)] = {
    QueryEnumerator(() => keysEnumerator.apply(Iteratee.getChunks[String]).flatMap(_.run).flatMap { keys =>
      waitForBulkRaw(bucket.couchbaseCluster.asyncGetBulk(keys), bucket, ec).map { results =>
        Enumerator.enumerate(results.toList)
      }.map { enumerator =>
        enumerator &> Enumeratee.collect[(String, AnyRef)] {
          case (k: String, v: String)                                => (k, v)
          case (k: String, v: AnyRef) if bucket.failWithNonStringDoc => throw new IllegalStateException(s"Document $k is not a String")
        }
      }
    })
  }*/

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
  def fetch[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    QueryEnumerator(() => rawFetch(keysEnumerator)(bucket, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, String)](t => (t._1, r.reads(Json.parse(t._2)))) &> Enumeratee.collect[(String, JsResult[T])] {
        case (k: String, JsSuccess(value, _))                            => (k, value)
        case (k: String, JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toJson(errors))
      }
    })
  }

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
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(() => fetch[T](keysEnumerator)(bucket, r, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[(String, T)](_._2)
    })
  }

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
  def fetch[T](keys: Seq[String])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = {
    fetch[T](Enumerator.enumerate(keys))(bucket, r, ec)
  }

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
    bucket.client.get(key).toFuture map {
      case json: JsonDocument => Some(r.reads(Json.parse(json.content().toString)))
      case _                  => None
    } map {
      case Some(JsSuccess(value, _))                            => Some(value)
      case Some(JsError(errors)) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toJson(errors))
      case None                                                 => None
    }
  }

  def getBinaryBlob(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] =
    bucket.client.get(key, classOf[BinaryDocument]).toFuture.map(_.content().toString(CharsetUtil.UTF_8))


  def getStringBlob(key: String)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[String] =
    bucket.client.get(key, classOf[StringDocument]).toFuture.map(_.content())

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

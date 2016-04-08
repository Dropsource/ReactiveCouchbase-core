package org.reactivecouchbase.client

import com.couchbase.client.deps.io.netty.buffer.{ByteBuf, Unpooled}
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.document._
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import org.reactivecouchbase.observables._
import org.reactivecouchbase.{Couchbase, CouchbaseBucket}
import play.api.libs.iteratee.{Enumerator, Iteratee}
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait for write operations
  */
trait Write {

  private def convertType[T, D](key: String, value: T, exp: Long = Constants.expiration)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Document[D] = {
    (w.writes(value) match {
      case s: JsString                          => JsonStringDocument.create(key, s.value, exp)
      case b: JsBoolean                         => JsonBooleanDocument.create(key, b.value, exp)
      case n: JsNumber if n.value.isValidLong   => JsonLongDocument.create(key, n.value.longValue(), exp)
      case n: JsNumber if n.value.isExactDouble => JsonDoubleDocument.create(key, n.value.doubleValue(), exp)
      case a: JsArray                           => JsonArrayDocument.create(key, JsonArray.fromJson(a.toString()), exp)
      case o: JsObject                          => JsonDocument.create(key, JsonObject.fromJson(o.toString()), exp)
    }).asInstanceOf[Document[D]]
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Set Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    *
    * Set a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @tparam D the type of the content
    * @return the operation status
    */
  def set[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[Document[D]] =
    bucket.client.upsert(convertType[T, D](key, value), persistTo, replicateTo).toFuture

  /**
    *
    * Set a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @tparam D the type of the content
    * @return the operation status
    */
  def setStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    data(Iteratee.fold(List[Future[Document[D]]]()) { (list, chunk) =>
      list :+ set[T, D](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Add Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  /**
    *
    * Add a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @tparam D the type of the stored value
    * @return the operation status
    */
  def add[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[Document[D]] = {
    val formattedValue = w.writes(value) match {
      case s: JsString                          => JsonStringDocument.create(key, s.value, exp)
      case b: JsBoolean                         => JsonBooleanDocument.create(key, b.value, exp)
      case n: JsNumber if n.value.isValidLong   => JsonLongDocument.create(key, n.value.longValue(), exp)
      case n: JsNumber if n.value.isExactDouble => JsonDoubleDocument.create(key, n.value.doubleValue(), exp)
      case a: JsArray                           => JsonArrayDocument.create(key, JsonArray.fromJson(a.toString()), exp)
      case o: JsObject                          => JsonDocument.create(key, JsonObject.fromJson(o.toString()), exp)
    }

    bucket.client.insert(formattedValue, persistTo, replicateTo).toFuture.collect {
      case d: Document[D]@unchecked => d
    }
  }

  /**
    *
    * Add a blob document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def addBinaryBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Document[ByteBuf]] = {
    bucket.client.insert(BinaryDocument.create(key, Unpooled.copiedBuffer(value, CharsetUtil.UTF_8), exp), persistTo, replicateTo).toFuture
  }

  /**
    *
    * Add a blob document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def addStringBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Document[String]] = {
    bucket.client.insert(StringDocument.create(key, value, exp), persistTo, replicateTo).toFuture
  }

  /**
    *
    * Add a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def addStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    data(Iteratee.fold(List[Future[Document[D]]]()) { (list, chunk) =>
      list :+ add[T, D](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
    *
    * Add a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def addStreamBinaryBlob(data: Enumerator[(String, String)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[Document[ByteBuf]]] = {
    data(Iteratee.fold(List[Future[Document[ByteBuf]]]()) { (list, chunk) =>
      list :+ Couchbase.addBinaryBlob(chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Replace Operations
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    *
    * Replace a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def replace[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[Document[D]] =
    bucket.client.replace(convertType[T, D](key, value, exp), persistTo, replicateTo).toFuture

  /**
    *
    * Replace a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def replaceBinaryBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Document[ByteBuf]] = {
    bucket.client.replace(BinaryDocument.create(key, Unpooled.copiedBuffer(value, CharsetUtil.UTF_8), exp), persistTo, replicateTo).toFuture
  }

  /**
    *
    * Replace a stream documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def replaceStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    data(Iteratee.fold(List[Future[Document[D]]]()) { (list, chunk) =>
      list :+ replace[T, D](chunk._1, chunk._2, exp, persistTo, replicateTo)(bucket, w, ec)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }

  /**
    *
    * Flush the current bucket
    *
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def flush()(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Boolean] = {
    bucket.client.bucketManager().toFuture.map(_.flush().toBlocking.single())
  }
}

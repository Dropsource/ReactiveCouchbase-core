package org.reactivecouchbase.client

import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.java.document.{Document, JsonDocument}
import com.couchbase.client.java.view.{AsyncViewRow, DesignDocument, ViewQuery}
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import org.reactivecouchbase.CouchbaseExpiration._
import org.reactivecouchbase.{Couchbase, CouchbaseBucket}
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._

import scala.Some
import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait containing the whole ReactiveCouchbase API to put on CouchbaseBucket
  */
trait BucketAPI {
  self: CouchbaseBucket =>

  def docName(name: String) = Couchbase.docName(name)(self)

  /**
    * Perform a Couchbase query on a view
    *
    * @param docName the name of the design doc
    * @param viewName the name of the view
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the list of docs
    */
  def find[T](docName: String, viewName: String)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](ViewQuery.from(docName, viewName))(self, r, ec)
  }

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the list of docs
    */
  def find[T](view: ViewQuery)(implicit r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Couchbase.find[T](view)(self, r, ec)
  }

  /**
    *
    * Poll query every n millisec
    *
    * @param doc the name of the design doc
    * @param view the view to query
    * @param everyMillis repeat every ...
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def pollQuery[T](doc: String, view: String, everyMillis: Long, filter: T => Boolean = { chunk: T => true })(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.pollQuery[T](ViewQuery.from(doc, view), everyMillis, filter)(self, r, ec)
  }

  /**
    *
    * Repeat query
    *
    * @param doc the name of the design doc
    * @param view the view to query
    * @param filter the filter for documents selection
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def repeatQuery[T](doc: String, view: String, filter: T => Boolean = { chunk: T => true }, trigger: Future[AnyRef] = Future.successful(Some))(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Couchbase.repeatQuery[T](ViewQuery.from(doc, view), trigger, filter)(self, r, ec)
  }

//  /**
//    *
//    * 'Tail -f' on a query
//    *
//    * @param doc the name of the design doc
//    * @param view the view to query
//    * @param extractor id extrator of natural insertion order
//    * @param from start from id
//    * @param every tail every
//    * @param unit unit of time
//    * @param r Json reader for type T
//    * @param ec ExecutionContext for async processing
//    * @tparam T type of the doc
//    * @return the query enumerator
//    */
//  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
//    Couchbase.tailableQuery[T](doc, view, extractor, from, every, unit)(() => driver.bucket(bucketName), r, ec)
//  }

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @return
    */
  def rawFetch(keysEnumerator: Enumerator[String])(implicit ec: ExecutionContext): QueryEnumerator[(String, String)] = Couchbase.rawFetch(keysEnumerator)(this, ec)

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetch[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keysEnumerator)(this, r, ec)

  /**
    *
    * Fetch a stream of documents
    *
    * @param keysEnumerator stream of keys
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetchValues[T](keysEnumerator: Enumerator[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keysEnumerator)(this, r, ec)

  /**
    *
    * Fetch a stream of documents
    *
    * @param keys the key of the documents
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetch[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[(String, T)] = Couchbase.fetch[T](keys)(this, r, ec)


  /**
    *
    * Fetch a stream of documents
    *
    * @param keys the key of the documents
    * @param r Json reader
    * @tparam T type of the doc
    * @return
    */
  def fetchValues[T](keys: Seq[String])(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.fetchValues[T](keys)(this, r, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param docName the name of the design doc
    * @param viewName the name of the view
    * @param ec ExecutionContext for async processing
    * @return the query enumerator
    */
  def rawSearch(docName: String, viewName: String)(implicit ec: ExecutionContext): QueryEnumerator[AsyncViewRow] = Couchbase.rawSearch(ViewQuery.from(docName, viewName))(this, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param ec ExecutionContext for async processing
    * @return the query enumerator
    */
  def rawSearch(view: ViewQuery)(implicit ec: ExecutionContext): QueryEnumerator[AsyncViewRow] = Couchbase.rawSearch(view)(this, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param docName the name of the design doc
    * @param viewName the name of the view
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def search[T](docName: String, viewName: String)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](ViewQuery.from(docName, viewName))(this, r, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def search[T](view: ViewQuery)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = Couchbase.search[T](view)(this, r, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param docName the name of the design doc
    * @param viewName the name of the view
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def searchValues[T](docName: String, viewName: String)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](ViewQuery.from(docName, viewName))(this, r, ec)

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def searchValues[T](view: ViewQuery)(implicit r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = Couchbase.searchValues[T](view)(this, r, ec)

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  /**
//    *
//    * Fetch a view
//    *
//    * @param docName the name of the design doc
//    * @param viewName the name of the view
//    * @param ec ExecutionContext for async processing
//    * @return the view
//    */
//  def view(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[View] = {
//    Couchbase.view(docName, viewName)(self, ec)
//  }
//
//  /**
//    *
//    * Fetch a spatial view
//    *
//    * @param docName the name of the design doc
//    * @param viewName the name of the view
//    * @param ec ExecutionContext for async processing
//    * @return the spatial view
//    */
//  def spatialView(docName: String, viewName: String)(implicit ec: ExecutionContext): Future[SpatialView] = {
//    Couchbase.spatialView(docName, viewName)(self, ec)
//  }

  /**
    *
    * Fetch a design document
    *
    * @param docName the name of the design doc
    * @param ec ExecutionContext for async processing
    * @return fetch design doc
    */
  def designDocument(docName: String)(implicit ec: ExecutionContext): Future[DesignDocument] = {
    Couchbase.designDocument(docName)(self, ec)
  }

  /**
    *
    * Create a design doc
    *
    * @param value the content of the design doc
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def createDesignDoc(value: DesignDocument, development: Boolean = false)(implicit ec: ExecutionContext): Future[DesignDocument] = {
    Couchbase.createDesignDoc(value, development)(self, ec)
  }

  /**
    *
    * Delete a design doc
    *
    * @param name name of the design doc
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def deleteDesignDoc(name: String, development: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean] = {
    Couchbase.deleteDesignDoc(name, development)(self, ec)
  }

//  /**
//    *
//    * Fetch keys stats
//    *
//    * @param key the key of the document
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def keyStats(key: String)(implicit ec: ExecutionContext): Future[Map[String, String]] = {
//    Couchbase.keyStats(key)(self, ec)
//  }

  /**
    *
    * fetch a document
    *
    * @param key the key of the document
    * @tparam T type of the doc
    * @return
    */
  def get[T](key: String)(implicit r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    Couchbase.get[T](key)(self, r, ec)
  }

  /**
    *
    *
    *
    * @param key
    * @param ec
    * @return
    */
  def getBinaryBlob(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    Couchbase.getBinaryBlob(key)(self, ec).map {
      case s: String => Some(s)
      case _         => None
    }
  }

//  def getInt(key: String)(implicit ec: ExecutionContext): Future[Int] = Couchbase.getInt(key)(self, ec)

//  def getLong(key: String)(implicit ec: ExecutionContext): Future[Long] = Couchbase.getLong(key)(self, ec)

//  def setInt(key: String, value: Int)(implicit ec: ExecutionContext): Future[OpResult] = Couchbase.setInt(key, value)(self, ec)

//  def setLong(key: String, value: Long)(implicit ec: ExecutionContext): Future[OpResult] = Couchbase.setLong(key, value)(self, ec)

  /**
    *
    * Increment a Long
    *
    * @param key key of the Int value
    * @param by increment of the value
    * @param ec ExecutionContext for async processing
    * @return
    */
  def incr(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.incr(key, by)(self, ec)


//  /**
//    *
//    * Decrement an Int
//    *
//    * @param key key of the Int value
//    * @param by the value to decrement
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def decr(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.decr(key, by)(self, ec)

//  /**
//    *
//    * Decrement a Long
//    *
//    * @param key key of the Long value
//    * @param by the value to decrement
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def decr(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.decr(key, by)(self, ec)

//  /**
//    *
//    * Increment and get an Int
//    *
//    * @param key key of the Int value
//    * @param by the value to increment
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def incrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.incrAndGet(key, by)(self, ec)
//
//  /**
//    *
//    * Increment and get a Long
//    *
//    * @param key key of the Long value
//    * @param by the value to increment
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def incrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.incrAndGet(key, by)(self, ec)
//
//  /**
//    *
//    * Decrement and get an Int
//    *
//    * @param key key of the Int value
//    * @param by the value to decrement
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def decrAndGet(key: String, by: Int)(implicit ec: ExecutionContext): Future[Int] = Couchbase.decrAndGet(key, by)(self, ec)
//
//  /**
//    *
//    * Decrement and get a Long
//    *
//    * @param key key of the Long value
//    * @param by the value to decrement
//    * @param ec ExecutionContext for async processing
//    * @return
//    */
//  def decrAndGet(key: String, by: Long)(implicit ec: ExecutionContext): Future[Long] = Couchbase.decrAndGet(key, by)(self, ec)

//  /**
//    *
//    *
//    *
//    * @param key
//    * @param value
//    * @param exp
//    * @param persistTo
//    * @param replicateTo
//    * @param ec
//    * @return
//    */
//  def setBlob(key: String, value: String, exp: CouchbaseExpirationTiming = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[OpResult] = {
//    Couchbase.javaSet(key, exp, value, persistTo, replicateTo, self, ec)
//  }

  /**
    *
    * Set a document
    *
    * @param key the key of the document
    * @param value the document
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def set[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[Document[D]] = {
    Couchbase.set[T, D](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    *
    * Set a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag

    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def setStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    Couchbase.setStream[T, D](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    * Add Binary Blob
    * @param key
    * @param value
    * @param exp
    * @param persistTo
    * @param replicateTo
    * @param ec
    * @return
    */
  def addBinaryBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[Document[ByteBuf]] = {
    Couchbase.addBinaryBlob(key, value, exp, persistTo, replicateTo)(self, ec)
  }

  /**
    * Add String Blob
    * @param key
    * @param value
    * @param exp
    * @param persistTo
    * @param replicateTo
    * @param ec
    * @return
    */
  def addStringBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[Document[String]] = {
    Couchbase.addStringBlob(key, value, exp, persistTo, replicateTo)(self, ec)
  }

  /**
    *
    * Add a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def add[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[Document[D]] = {
    Couchbase.add[T, D](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    *
    * Add a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def addStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    Couchbase.addStream[T, D](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    *
    * Add a stream of documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def addStreamBinaryBlob(data: Enumerator[(String, String)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[List[Document[ByteBuf]]] = {
    Couchbase.addStreamBinaryBlob(data, exp, persistTo, replicateTo)(self, ec)
  }

  /**
    *
    *
    *
    * @param key
    * @param value
    * @param exp
    * @param persistTo
    * @param replicateTo
    * @param ec
    * @return
    */
  def replaceBinaryBlob(key: String, value: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[Document[ByteBuf]] = {
    Couchbase.replaceBinaryBlob(key, value, exp, persistTo, replicateTo)(self, ec)
  }

  /**
    *
    * Replace a document
    *
    * @param key the key of the document
    * @param value the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def replace[T, D](key: String, value: T, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[Document[D]] = {
    Couchbase.replace[T, D](key, value, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    *
    * Replace a stream documents
    *
    * @param data the stream of documents
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param w Json writer for type T
    * @param ec ExecutionContext for async processing
    * @tparam T the type of the doc
    * @return the operation status
    */
  def replaceStream[T, D](data: Enumerator[(String, T)], exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit w: Writes[T], ec: ExecutionContext): Future[List[Document[D]]] = {
    Couchbase.replaceStream[T, D](data, exp, persistTo, replicateTo)(self, w, ec)
  }

  /**
    *
    * Delete a document
    *
    * @param key the key to delete
    * @param persistTo persist flag
    * @param replicateTo repplication flag
    * @return the operation status for the delete operation
    */
  def delete(key: String, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE): Future[JsonDocument] = {
    Couchbase.delete(key, persistTo, replicateTo)(self)
  }

  /**
    *
    * Delete a stream of documents
    *
    * @param data the stream of documents to delete
    * @param persistTo persist flag
    * @param replicateTo repplication flag
    * @param ec ExecutionContext for async processing
    * @return the operation statuses for the delete operation
    */
  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit ec: ExecutionContext): Future[List[JsonDocument]] = {
    Couchbase.deleteStream(data, persistTo, replicateTo)(self, ec)
  }

  /**
    *
    * Flush the current bucket
    *
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def flush()(implicit ec: ExecutionContext): Future[Boolean] = Couchbase.flush()(self, ec)

  /**
    *
    * Get a doc and lock it
    *
    * @param key key of the lock
    * @param exp expiration of the lock
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return Cas value
    */
  def getAndLock[T](key: String, exp: CouchbaseExpirationTiming)(implicit r: Reads[T], ec: ExecutionContext): Future[JsonDocument] = {
    Couchbase.getAndLock(key, exp)(r, self, ec)
  }

  /**
    *
    * Unlock a locked key
    *
    * @param key key to unlock
    * @param casId id of the compare and swap operation
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def unlock(key: String, casId: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    Couchbase.unlock(key, casId)(self, ec)
  }

//  /**
//    *
//    * Atomically perform operation(s) on a document while it's locked
//    *
//    * @param key the key of the document
//    * @param operation the operation(s) to perform on the document while it's locked
//    * @param ec ExecutionContext for async processing
//    * @param r Json reader
//    * @param w Json writer
//    * @tparam T type of the doc
//    * @return the document
//    */
  //def atomicUpdate[T](key: String, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => T)(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
  //  Couchbase.atomicUpdate[T](key, persistTo, replicateTo)(operation)(self, ec, r, w)
  //}

//  /**
//    *
//    * Atomically perform async operation(s) on a document while it's locked
//    *
//    * @param key the key of the document
//    * @param operation the async operation(s) to perform on the document while it's locked
//    * @param ec ExecutionContext for async processing
//    * @param r Json reader
//    * @param w Json writer
//    * @tparam T type of the doc
//    * @return the document
//    */
  //def atomicallyUpdate[T](key: String, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => Future[T])(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
  //  Couchbase.atomicallyUpdate[T](key, persistTo, replicateTo)(operation)(self, ec, r, w)
  //}

  def atomicallyUpdate[T](key: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => Future[T])(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    Couchbase.atomicallyUpdate[T](key, exp, persistTo, replicateTo)(operation)(self, ec, r, w)
  }

  def atomicUpdate[T](key: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => T)(implicit ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    Couchbase.atomicUpdate[T](key, exp, persistTo, replicateTo)(operation)(self, ec, r, w)
  }

}

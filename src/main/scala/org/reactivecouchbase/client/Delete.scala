package org.reactivecouchbase.client

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import org.reactivecouchbase.CouchbaseBucket
import org.reactivecouchbase.observables._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import rx.lang.scala.JavaConversions._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait for delete operations
  */
trait Delete {

  /**
    *
    * Delete a document
    *
    * {{{
    *   val driver = ReactiveCouchbaseDriver()
    *   implicit val bucket = driver.bucket("default")
    *   implicit val ec = import scala.concurrent.ExecutionContext.Implicits.global
    *
    *   Couchbase.delete("my-key").map { operationStatus =>
    *     println(s"Delete done ${operationStatus.getMessage}")
    *   }
    * }}}
    *
    * @param key the key to delete
    * @param persistTo persist flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @return the operation status for the delete operation
    */
  def delete(key: String, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket): Future[JsonDocument] =
    toScalaObservable(bucket.client.remove(key, persistTo, replicateTo)).toFuture

  /**
    *
    * Delete a stream of documents
    *
    * @param data the stream of documents to delete
    * @param persistTo persist flag
    * @param replicateTo repplication flag
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation statuses for the delete operation
    */
  def deleteStream(data: Enumerator[String], persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[List[JsonDocument]] = {
    data(Iteratee.fold(List[Future[JsonDocument]]()) { (list, chunk) =>
      list :+ delete(chunk, persistTo, replicateTo)(bucket)
    }).flatMap(_.run).flatMap(Future.sequence(_))
  }
}

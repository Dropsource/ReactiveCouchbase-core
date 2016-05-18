package org.reactivecouchbase.client

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.view._
import org.reactivecouchbase.observables._
import org.reactivecouchbase.{CouchbaseBucket, Timeout}
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}
import play.api.libs.json.{JsSuccess, _}
import rx.lang.scala.JavaConversions._

import scala.Some
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  *
  * Js row query result
  *
  * @param document the document
  * @param id the documents key
  * @param key the documents indexed key
  * @param value the documents indexed value
  * @tparam T type of the doc the type of the doc
  */
case class JsRow[T](document: JsResult[T], id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
  *
  * Typed row query result
  *
  * @param document the document
  * @param id the documents key
  * @param key the documents indexed key
  * @param value the documents indexed value
  * @tparam T type of the doc the type of the doc
  */
case class TypedRow[T](document: T, id: Option[String], key: String, value: String) {
  def toTuple = (document, id, key, value)
}

/**
  *
  * ReactiveCouchbase representation of a query result
  *
  * @param futureEnumerator doc stream
  * @tparam T type of the doc type of doc
  */
class QueryEnumerator[T](futureEnumerator: () => Future[Enumerator[T]]) {

  /**
    * @return the enumerator for query results
    */
  def toEnumerator: Future[Enumerator[T]] = futureEnumerator()

  /**
    *
    * @param ec ExecutionContext for async processing
    * @return the query result as enumerator
    */
  def enumerate(implicit ec: ExecutionContext): Enumerator[T] =
    Enumerator.flatten(futureEnumerator())

  /**
    *
    * @param ec ExecutionContext for async processing
    * @return the query result as list
    */
  def toList(implicit ec: ExecutionContext): Future[List[T]] =
    futureEnumerator().flatMap(_ (Iteratee.getChunks[T]).flatMap(_.run))

  /**
    *
    * @param ec ExecutionContext for async processing
    * @return the optinal head
    */
  def headOption(implicit ec: ExecutionContext): Future[Option[T]] =
    futureEnumerator().flatMap(_ (Iteratee.head[T]).flatMap(_.run))
}

/**
  * Companion object to build QueryEnumerators
  */
object QueryEnumerator {
  def apply[T](enumerate: () => Future[Enumerator[T]]): QueryEnumerator[T] = new QueryEnumerator[T](enumerate)
}

/**
  * Trait to query Couchbase
  */
trait Queries {

  def docName(name: String)(implicit bucket: CouchbaseBucket) = {
    s"${bucket.cbDriver.mode}$name"
  }

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the list of docs
    */
  def find[T](view: ViewQuery)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext) = searchValues(view)(bucket, r, ec).toList(ec)

  ///////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param viewQuery the view to query
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the query enumerator
    */
  def rawSearch(viewQuery: ViewQuery)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): QueryEnumerator[AsyncViewRow] = {
    QueryEnumerator(() => toScalaObservable(bucket.client.query(viewQuery)).flatMap(_.rows().toList).toFuture.map { rows =>
      Enumerator.enumerate(rows.asScala.iterator)
    })
  }

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def search[T](view: ViewQuery)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[TypedRow[T]] = {
    QueryEnumerator(() => rawSearch(view.includeDocs(true))(bucket, ec).toEnumerator.map { enumerator =>
      enumerator &>
        Enumeratee.map[AsyncViewRow] { row =>
          Try(row.document.toBlocking.first()) match {
            case Success(doc: JsonDocument) =>
              JsRow[T](r.reads(Json.parse(doc.content().toString)), Option(row.id), row.key().toString, Option(row.value).fold("")(_.toString))
            case Failure(exc)               =>
              JsRow[T](JsError(), Option(row.id()), row.key().toString, row.value().toString)
          }
        } &>
        Enumeratee.collect[JsRow[T]] {
          case JsRow(JsSuccess(doc, _), id, key, value)                       => TypedRow[T](doc, id, key, value)
          case JsRow(JsError(errors), _, _, _) if bucket.jsonStrictValidation => throw new JsonValidationException("Invalid JSON content", JsError.toFlatJson(errors))
        }
    })
  }

  /**
    *
    * Perform a Couchbase query on a view
    *
    * @param view the view to query
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def searchValues[T](view: ViewQuery)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): QueryEnumerator[T] = {
    QueryEnumerator(() => search[T](view)(bucket, r, ec).toEnumerator.map { enumerator =>
      enumerator &> Enumeratee.map[TypedRow[T]](_.document)
    })
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
  //    * @param bucket the bucket to use
  //    * @param r Json reader for type T
  //    * @param ec ExecutionContext for async processing
  //    * @tparam T type of the doc
  //    * @return the query enumerator
  //    */
  //  def tailableQuery[T](doc: String, view: String, extractor: T => Long, from: Long = 0L, every: Long = 1000L, unit: TimeUnit = TimeUnit.MILLISECONDS)(implicit bucket: () => CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
  //    var last = System.currentTimeMillis()
  //    def query() = new Query()
  //                  .setIncludeDocs(true)
  //                  .setStale(Stale.FALSE)
  //                  .setDescending(false)
  //                  .setRangeStart(ComplexKey.of(last.asInstanceOf[AnyRef]))
  //                  .setRangeEnd(ComplexKey.of(Long.MaxValue.asInstanceOf[AnyRef]))
  //
  //    def step(list: ConcurrentLinkedQueue[T]): Future[Option[(ConcurrentLinkedQueue[T], T)]] = {
  //      Couchbase.find[T](doc, view)(query())(bucket(), r, ec).map { res =>
  //        res.foreach { doc =>
  //          last = extractor(doc) + 1L
  //          list.offer(doc)
  //        }
  //      }.flatMap { _ =>
  //        list.poll() match {
  //          case null => Timeout.timeout("", every, unit, bucket().driver.scheduler()).flatMap(_ => step(list))
  //          case e    => Future.successful(Some((list, e)))
  //        }
  //      }
  //    }
  //    Enumerator.unfoldM(new ConcurrentLinkedQueue[T]()) { list =>
  //      if (list.isEmpty) {
  //        step(list)
  //      } else {
  //        val el = list.poll()
  //        Future.successful(Some((list, el)))
  //      }
  //    }
  //  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    *
    * Poll query every n millisec
    *
    * @param viewQuery the actual query
    * @param everyMillis repeat every ...
    * @param filter the filter for documents selection
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def pollQuery[T](viewQuery: ViewQuery, everyMillis: Long, filter: T => Boolean = { chunk: T => true })(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      Timeout.timeout(Some, everyMillis, TimeUnit.MILLISECONDS, bucket.driver.scheduler()).flatMap(_ => find[T](viewQuery)(bucket, r, ec))
    ).through(Enumeratee.mapConcat[List[T]](identity)).through(Enumeratee.filter[T](filter))
  }

  /**
    *
    * Repeat a query each time trigger is done
    *
    * @param viewQuery the actual query
    * @param trigger trigger the repeat when future is done
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def repeatQuery[T](viewQuery: ViewQuery, trigger: Future[AnyRef])(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    repeatQuery[T](viewQuery, trigger, { chunk: T => true })(bucket, r, ec)
  }

  /**
    *
    * Repeat a query each time trigger is done
    *
    * @param viewQuery the actual query
    * @param trigger trigger the repeat when future is done
    * @param filter the filter for documents selection
    * @param bucket the bucket to use
    * @param r Json reader for type T
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return the query enumerator
    */
  def repeatQuery[T](viewQuery: ViewQuery, trigger: Future[AnyRef], filter: T => Boolean)(implicit bucket: CouchbaseBucket, r: Reads[T], ec: ExecutionContext): Enumerator[T] = {
    Enumerator.repeatM(
      trigger.flatMap { _ => find[T](viewQuery)(bucket, r, ec) }
    ).through(Enumeratee.mapConcat[List[T]](identity)).through(Enumeratee.filter[T](filter))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    *
    * Fetch a design document
    *
    * @param docName the name of the design doc
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return fetch design doc
    */
  def designDocument(docName: String, development: Boolean = false)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] = {
    toScalaObservable(bucket.client.bucketManager()).toFuture.flatMap { manager =>
      toScalaObservable(manager.getDesignDocument(docName, development)).toFuture
    }
  }

  /**
    *
    * Create a design doc
    *
    * @param designDoc the content of the design doc
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def createDesignDoc(designDoc: DesignDocument, development: Boolean)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[DesignDocument] =
    toScalaObservable(bucket.client.bucketManager()).toFuture.flatMap { manager =>
      toScalaObservable(manager.insertDesignDocument(designDoc, development)).toFuture
    }

  /**
    *
    * Delete a design doc
    *
    * @param name name of the design doc
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def deleteDesignDoc(name: String, development: Boolean)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Boolean] =
    toScalaObservable(bucket.client.bucketManager()).toFuture.flatMap { manager =>
      toScalaObservable(manager.removeDesignDocument(name, development)).toFuture.map(_.booleanValue())
    }
}

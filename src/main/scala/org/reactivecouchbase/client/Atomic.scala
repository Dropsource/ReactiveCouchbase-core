package org.reactivecouchbase.client

import akka.actor.{Props, _}
import akka.pattern.{after, _}
import akka.util.Timeout
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import org.reactivecouchbase.observables._
import org.reactivecouchbase.{CouchbaseBucket, LoggerLike}
import play.api.libs.json._
import rx.lang.scala.JavaConversions._

import scala.Some
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.ControlThrowable
import scala.util.{Failure, Success, Try}

/**
  * @author Quentin ADAM - @waxzce - https://github.com/waxzce
  */

private[reactivecouchbase] case class AtomicRequest[T](key: String, operation: T => Future[T], bucket: CouchbaseBucket, atomic: Atomic, r: Reads[T], w: Writes[T], ec: ExecutionContext, numberTry: Int, persistTo: PersistTo, replicateTo: ReplicateTo, expiration: Option[Long])

private[reactivecouchbase] case class AtomicSuccess[T](key: String, payload: T)

private[reactivecouchbase] case class LoggerHolder(logger: LoggerLike)

case class AtomicError[T](request: AtomicRequest[T], message: String) extends ControlThrowable

case class AtomicTooMuchTryError[T](request: AtomicRequest[T]) extends ControlThrowable

case class AtomicNoKeyFoundError[T](request: AtomicRequest[T]) extends ControlThrowable

case class AtomicWeirdError() extends ControlThrowable

private[reactivecouchbase] object AtomicActor {
  def props[T]: Props = Props(classOf[AtomicActor[T]])
}

private[reactivecouchbase] class AtomicActor[T] extends Actor {

  var logger: Option[LoggerLike] = None

  def receive = {
    case lh: LoggerHolder     => logger = Some(lh.logger)
    case ar: AtomicRequest[T] => {
      // I need some implicit, I know it's not good looking
      implicit val rr = ar.r
      implicit val ww = ar.w
      implicit val bb = ar.bucket
      implicit val ee = ar.ec
      // define other implicit
      implicit val timeout = Timeout(8 minutes)
      // backup my sender, need it later, and actor shared state is not helping...
      val sen = sender

      if (ar.numberTry < 15) {

        val myresult = ar.atomic.getAndLock(ar.key, 30).onComplete {
          case Success(jsonDocument)                     => {
            // \o/ we successfully lock the key
            // get current object
            val cv = ar.r.reads(Json.parse(jsonDocument.content().toString)).get
            // apply transformation and get new object
            //val nv = ar.operation(cv)
            ar.operation(cv).map { nv =>
              // write new object to couchbase and unlock :-)
              // TODO : use asyncCAS method, better io management
              Try(ar.bucket.client.unlock(jsonDocument)) match {
                case Success(unlocked) =>
                  sen ! Future.successful(AtomicSuccess[T](ar.key, nv))
                  self ! PoisonPill
                case Failure(exc)      => exc match {
                  case e: DocumentDoesNotExistException =>
                    sen ! Future.failed(throw new AtomicNoKeyFoundError[T](ar))
                    self ! PoisonPill
                  case e: Exception                     =>
                    sen ! Future.failed(throw new AtomicError[T](ar, e.getMessage))
                    self ! PoisonPill
                }
              }

              // reply to sender it's OK
              /*res match {
                case CASResponse.OK => {
                                  sen ! Future.successful(AtomicSuccess[T](ar.key, nv))
                                  self ! PoisonPill
                                }
                                case CASResponse.NOT_FOUND => {
                                  sen ! Future.failed(throw new AtomicNoKeyFoundError[T](ar))
                                  self ! PoisonPill
                                }
                case CASResponse.EXISTS => {
                  // something REALLY weird append during the locking time. But anyway, we can retry :-)
                  bb.driver.logger.warn("Couchbase lock WEIRD error : desync of CAS value. Retry anyway")
                  val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1, ar.persistTo, ar.replicateTo, ar.expiration)
                  val atomic_actor = bb.driver.system().actorOf(AtomicActor.props[T])
                  for (
                    tr <- (atomic_actor ? ar2)
                  ) yield (sen ! tr)
                  self ! PoisonPill
                }
                                case _ => {
                                  sen ! Future.failed(throw new AtomicError[T](ar, res.name))
                                  self ! PoisonPill
                                }
              }*/
            }
          }
            // TODO not sure
          /*case Failure(ex: OperationStatusErrorNotFound) => {
            sen ! Future.failed(new AtomicNoKeyFoundError[T](ar))
            self ! PoisonPill
          }*/
          case r                                         => {
            // Too bad, the object is not locked and some else is working on it...
            // build a new atomic request
            val ar2 = AtomicRequest(ar.key, ar.operation, ar.bucket, ar.atomic, ar.r, ar.w, ar.ec, ar.numberTry + 1, ar.persistTo, ar.replicateTo, ar.expiration)
            // get my actor
            val atomic_actor = bb.driver.system().actorOf(AtomicActor.props[T])
            // wait and retry by asking actor with new atomic request
            for (
              d <- after(200 millis, using =
                context.system.scheduler)(Future.successful(ar2));
              tr <- (atomic_actor ? d)
            // send result :-)
            ) yield (sen ! tr)
            self ! PoisonPill
          }
        }
      } else {
        sen ! Future.failed(throw new AtomicTooMuchTryError[T](ar))
        self ! PoisonPill
      }
    }
    case _                    => {
      logger.map(_.error("An atomic actor get a message, but not a atomic request, it's weird ! "))
      sender ! Future.failed(throw new AtomicWeirdError())
      self ! PoisonPill
    }
  }
}

/**
  * Trait for Atomic updates
  */
trait Atomic {

  /**
    *
    * Get a doc and lock it
    *
    * @param key key of the lock
    * @param exp expiration of the lock
    * @param r Json reader for type T
    * @param bucket the bucket used
    * @param ec ExecutionContext for async processing
    * @tparam T type of the doc
    * @return Cas value
    */
  def getAndLock[T](key: String, exp: Int)(implicit r: Reads[T], bucket: CouchbaseBucket, ec: ExecutionContext): Future[JsonDocument] = {
    toScalaObservable(bucket.client.getAndLock(key, exp)).toFuture
  }

  /**
    *
    * Unlock a locked key
    *
    * @param key key to unlock
    * @param casId id of the compare and swap operation
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @return the operation status
    */
  def unlock(key: String, casId: Long)(implicit bucket: CouchbaseBucket, ec: ExecutionContext): Future[Boolean] = {
    toScalaObservable(bucket.client.unlock(key, casId)).toFuture.map(_.booleanValue())
  }

  /**
    *
    * Atomically perform async operation(s) on a document while it's locked
    *
    * @param key the key of the document
    * @param exp expiration of the doc
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param operation the async operation(s) to perform on the document while it's locked
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @param r Json reader
    * @param w Json writer
    * @tparam T type of the doc
    * @return the document
    */
  def atomicallyUpdate[T](key: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => Future[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    performAtomicUpdate(key, Some(exp), persistTo, replicateTo)(operation)(bucket, ec, r, w)
  }

  private[reactivecouchbase] def performAtomicUpdate[T](key: String, exp: Option[Long], persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => Future[T])(implicit bucket: CouchbaseBucket, ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    implicit val timeout = Timeout(8 minutes)
    val ar = AtomicRequest[T](key, operation, bucket, this, r, w, ec, 1, persistTo, replicateTo, exp)
    val atomic_actor = bucket.driver.system().actorOf(AtomicActor.props[T])
    atomic_actor ! LoggerHolder(bucket.driver.logger)
    atomic_actor.ask(ar).flatMap {
      case f: Future[_] => f
      case other        => Future.successful(other)
    }.map {
      case AtomicSuccess(_, something) => something
      case other                       => other
    }.map(_.asInstanceOf[T])
  }

  /**
    *
    * Atomically perform operation(s) on a document while it's locked
    *
    * @param key the key of the document
    * @param exp expiration of the doc
    * @param operation the operation(s) to perform on the document while it's locked
    * @param persistTo persistence flag
    * @param replicateTo replication flag
    * @param bucket the bucket to use
    * @param ec ExecutionContext for async processing
    * @param r Json reader
    * @param w Json writer
    * @tparam T type of the doc
    * @return the document
    */
  def atomicUpdate[T](key: String, exp: Long = Constants.expiration, persistTo: PersistTo = PersistTo.NONE, replicateTo: ReplicateTo = ReplicateTo.NONE)(operation: T => T)(implicit bucket: CouchbaseBucket, ec: ExecutionContext, r: Reads[T], w: Writes[T]): Future[T] = {
    performAtomicUpdate(key, Some(exp), persistTo, replicateTo)((arg: T) => Future(operation(arg)))(bucket, ec, r, w)
  }

}


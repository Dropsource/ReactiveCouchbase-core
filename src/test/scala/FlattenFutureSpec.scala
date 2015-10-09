import java.util.concurrent.TimeUnit

import org.specs2.execute.Result
import org.specs2.main.CommandLine
import org.specs2.mutable.Specification

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class FlattenFutureSpec extends Specification {
  sequential

  import org.reactivecouchbase.implicits.flatfutures._

  val successfulFuture: Future[Option[String]] = Future.successful(Some("Hello"))
  val failedFuture: Future[Option[String]] = Future.successful(None)
  implicit val ec = ExecutionContext.Implicits.global

  def successIfSuccess[A](value: A) = new org.specs2.main.CommandLineAsResult[Future[A]] {

    def asResult(commandLine: CommandLine, r: => Future[A]): Result = {
      try {
        val res: A = Await.result(r, Duration(10, TimeUnit.SECONDS))
        (value shouldEqual res).toResult
      } catch {
        case e: Throwable => failure("Future failed")
      }
    }
  }

  def successIfFail() = new org.specs2.main.CommandLineAsResult[Future[String]] {
    def asResult(commandLine: CommandLine, r: => Future[String]): Result = {
      try {
        Await.result(r, Duration(10, TimeUnit.SECONDS))
        failure("Future is successful")
      } catch {
        case EmptyOption => success
        case _: Throwable => failure("Not an EmptyOption")
      }
    }
  }

  "Future of option" should {

    "Success if future of some".in {
      successfulFuture.flatten
    }(successIfSuccess("Hello"))

    "Success if future of some with transformation".in {
      successfulFuture.flatten(_.toUpperCase)("Goobdye")
    }(successIfSuccess("HELLO"))

    "Success if future of some with monadic transformation".in {
      successfulFuture.flattenM(v => Future.successful(v.toUpperCase))(Future.successful("Goobdye"))
    }(successIfSuccess("HELLO"))

    "Success if future of None".in {
      failedFuture.flatten("Goodbye")
    }(successIfSuccess("Goodbye"))

    "Success if future of None monadic".in {
      failedFuture.flattenM(Future.successful("Goodbye"))
    }(successIfSuccess("Goodbye"))

    "Fail if future of None".in {
      failedFuture.flatten
    }(successIfFail())
  }
}
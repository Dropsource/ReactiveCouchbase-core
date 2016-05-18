package org.reactivecouchbase.client

import play.api.libs.json.JsValue

case class OpResult(ok: Boolean, msg: Option[String], document: Option[JsValue], updated: Int) {
  def isSuccess = ok
  def isFailure = !ok
  def getMessage = msg.getOrElse("No message !!!")
  def getMessage(mess: String) = msg.getOrElse(mess)
}

object OpResult {

}

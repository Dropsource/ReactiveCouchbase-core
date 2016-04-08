package org.reactivecouchbase.experimental

import play.api.libs.json._

/**
 *
 * Metadata for each retrieved document
 *
 * @param id the key of the document
 * @param rev the revision of the document
 * @param expiration the expiration (UNIX epoch ???)
 * @param flags flags of the document
 */
case class Meta(id: String, rev: String, expiration: Long, flags: Long)

/**
 *
 * Representation of a document from a Query (with raw values)
 *
 * @param document maybe contains the value of the stored document
 * @param id maybe the key of the document (can be None if reduced query)
 * @param key the index key of the document
 * @param value the value
 * @param meta the metadata of the doc
 */
case class ViewRow(document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta) {
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}

/**
 *
 * Representation of a document from a Query (with typed values)
 *
 * @param document maybe contains the value of the stored document
 * @param id maybe the key of the document (can be None if reduced query)
 * @param key the index key of the document
 * @param value the value
 * @param meta the metadata of the doc
 * @param reader Json reader for the document
 * @tparam T the type of the document
 */
case class TypedViewRow[T](document: Option[JsValue], id: Option[String], key: String, value: String, meta: Meta, reader: Reads[T]) {
  def JsResult: JsResult[T] = document.map( doc => reader.reads(doc)).getOrElse(JsError())  // TODO : cache it
  def Instance: Option[T] = JsResult match {  // TODO : cache it
    case JsSuccess(doc, _) => Some(doc)
    case JsError(errors) => None
  }
  def toTuple = (document, id, key, value, meta)
  def withReads[A](r: Reads[A]): TypedViewRow[A] = TypedViewRow[A](document, id, key, value, meta, r)
}

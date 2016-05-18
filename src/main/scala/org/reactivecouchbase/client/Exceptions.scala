package org.reactivecouchbase.client

import play.api.libs.json.{JsObject, Json}

/**
 *
 * When the JSON format isn't good
 *
 * @param message
 * @param errors
 */
class JsonValidationException(message: String, errors: JsObject) extends ReactiveCouchbaseException("Json Validation failed", message + " : " + Json.stringify(errors))

/**
 *
 * Standard ReactiveCouchbase Exception
 *
 * @param title
 * @param message
 */
class ReactiveCouchbaseException(title: String, message: String) extends RuntimeException(title + " : " + message)
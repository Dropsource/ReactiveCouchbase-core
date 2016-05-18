package org.reactivecouchbase.client

import com.couchbase.client.java.{ReplicateTo, PersistTo}

/**
 * Some constants
 */
object Constants {

  /**
   * Infinity persistence
   */
  val expiration: Long = 0

  /**
   * Standard PersistTo
   */
  implicit val defaultPersistTo: PersistTo = PersistTo.NONE

  /**
   * Standard ReplicateTo
   */
  implicit val defaultReplicateTo: ReplicateTo = ReplicateTo.NONE
}
package com.evolution.scache

import scala.util.control.NoStackTrace

/**
 * Failure of a value computation that was taking so long the entry got expired while still loading,
 * see [[ExpiringCache]].
 */
case object ExpiredError extends RuntimeException with NoStackTrace

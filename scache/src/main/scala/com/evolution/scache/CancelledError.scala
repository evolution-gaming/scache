package com.evolution.scache

import scala.util.control.NoStackTrace

/**
 * Failure of a value computation that got cancelled, reported to the callers that were waiting for
 * its result rather than running it themselves.
 */
case object CancelledError extends RuntimeException with NoStackTrace

package com.evolutiongaming.scache

import scala.util.control.NoStackTrace

case object CacheReleasedError extends RuntimeException with NoStackTrace

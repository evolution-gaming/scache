package com.evolution.scache

import scala.util.control.NoStackTrace

case object CancelledError extends RuntimeException with NoStackTrace

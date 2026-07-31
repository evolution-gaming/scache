package com.evolution.scache

import scala.util.control.NoStackTrace

case object ExpiredError extends RuntimeException with NoStackTrace

package org.ardlema.solutions.joining

import java.time.Instant

trait Clock {

  def now(): Instant

}

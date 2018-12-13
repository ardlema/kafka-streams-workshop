package org.ardlema.joining

import java.time.Instant

trait Clock {

  def now(): Instant

}

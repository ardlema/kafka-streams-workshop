package org.ardlema.solutions.joining

import java.time.Instant

trait SystemClock extends Clock {

  def now(): Instant = Instant.now()

}

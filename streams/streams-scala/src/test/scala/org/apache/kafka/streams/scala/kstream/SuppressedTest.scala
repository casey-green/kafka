package org.apache.kafka.streams.scala.kstream
import java.time.Duration

import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.kstream.internals.suppress.{BufferFullStrategy, FinalResultsSuppressionBuilder, SuppressedInternal}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class SuppressedTest extends FlatSpec with Matchers {

  "Suppressed.untilWindowCloses" should "produce the correct suppression" in {
    val bufferConfig = BufferConfig.unbounded()
    val suppression = Suppressed.untilWindowCloses[String](bufferConfig)
    suppression shouldEqual new FinalResultsSuppressionBuilder(null, bufferConfig)
    suppression.withName("soup") shouldEqual new FinalResultsSuppressionBuilder("soup", bufferConfig)
  }

  "Suppressed.untilTimeLimit" should "produce the correct suppression" in {
    val bufferConfig = BufferConfig.unbounded()
    val duration = Duration.ofMillis(1)
    Suppressed.untilTimeLimit[String](duration, bufferConfig) shouldEqual
      new SuppressedInternal[String](null, duration, bufferConfig, null, false)
  }

  "BufferConfig.unbounded" should "produce the correct buffer config" in {
    BufferConfig.unbounded() shouldEqual
      BufferConfig.maxBytes(Long.MaxValue).withMaxRecords(Long.MaxValue).shutDownWhenFull()
  }

  "BufferConfig" should "work, and support very long chains of factory methods" in {
    val bc1 = BufferConfig
      .unbounded()
      .emitEarlyWhenFull()
      .withMaxRecords(3L)
      .withMaxBytes(4L)
      .withMaxRecords(5L)
      .withMaxBytes(6L)

    bc1.maxBytes() shouldEqual 6L
    bc1.maxRecords() shouldEqual 5L
    bc1.bufferFullStrategy() shouldEqual BufferFullStrategy.EMIT

    val bc2 = BufferConfig
      .maxBytes(4L)
      .withMaxRecords(5L)
      .withMaxBytes(6L)
      .withNoBound()
      .withMaxBytes(7L)
      .withMaxRecords(8L)

    bc2.maxBytes() shouldEqual 7L
    bc2.maxRecords() shouldEqual 8L
    bc2.bufferFullStrategy() shouldEqual BufferFullStrategy.SHUT_DOWN

    val bc3 = BufferConfig
      .maxRecords(5L)
      .withMaxBytes(10L)
      .emitEarlyWhenFull()
      .withMaxRecords(11L)

    bc3.maxRecords() shouldEqual 11L
    bc3.maxBytes() shouldEqual 10L
    bc3.bufferFullStrategy() shouldEqual BufferFullStrategy.EMIT
  }

}

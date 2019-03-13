/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;
import org.junit.Test;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SuppressedTest {

    @Test
    public void bufferBuilderShouldBeConsistent() {
        final Suppressed.BufferConfig bc0 = maxBytes(2L).withMaxRecords(4L).withNoBound();
        assertThat(
            "noBound should remove bounds",
            bc0,
            is(unbounded())
        );
        assertThat(
            "noBound should shutdown when full",
            bc0.bufferFullStrategy(),
            is(SHUT_DOWN)
        );

        final Suppressed.BufferConfig bc1 = maxRecords(2L);
        assertThat(
                "keys are set.",
                bc1.maxRecords(),
                is(2L)
        );
        assertThat(
                "bytes are not set.",
                bc1.maxBytes(),
                is(MAX_VALUE)
        );

        final Suppressed.BufferConfig bc2 = maxBytes(2L);
        assertThat(
            "size alone should be set",
            bc2.maxBytes(),
            is(2L)
        );
        assertThat(
            "size alone should be set",
            bc2.maxRecords(),
            is(MAX_VALUE)
        );
        assertThat(
            "Buffer strategy is emit",
            bc2.bufferFullStrategy(),
            is(BufferFullStrategy.EMIT)
        );
    }

    @Test
    public void intermediateEventsShouldAcceptAnyBufferAndSetBounds() {
        assertThat(
            "name should be set",
            untilTimeLimit(ofMillis(2), unbounded()).withName("myname"),
            is(new SuppressedInternal<>("myname", ofMillis(2), unbounded(), null, false))
        );

        assertThat(
            "time alone should be set",
            untilTimeLimit(ofMillis(2), unbounded()),
            is(new SuppressedInternal<>(null, ofMillis(2), unbounded(), null, false))
        );

        assertThat(
            "time and unbounded buffer should be set",
            untilTimeLimit(ofMillis(2), unbounded()),
            is(new SuppressedInternal<>(null, ofMillis(2), unbounded(), null, false))
        );

        assertThat(
            "time and keys buffer should be set",
            untilTimeLimit(ofMillis(2), maxRecords(2)),
            is(new SuppressedInternal<>(null, ofMillis(2), maxRecords(2), null, false))
        );

        assertThat(
            "time and size buffer should be set",
            untilTimeLimit(ofMillis(2), maxBytes(2)),
            is(new SuppressedInternal<>(null, ofMillis(2), maxBytes(2), null, false))
        );

        assertThat(
            "all constraints should be set",
            untilTimeLimit(ofMillis(2L), maxRecords(3L).withMaxBytes(2L)),
            is(new SuppressedInternal<>(null, ofMillis(2), maxRecords(3L).withMaxBytes(2L), null, false))
        );
    }

    @Test
    public void finalEventsShouldAcceptStrictBuffersAndSetBounds() {

        assertThat(
            untilWindowCloses(unbounded()),
            is(new FinalResultsSuppressionBuilder<>(null, unbounded()))
        );

        assertThat(
            untilWindowCloses(maxRecords(2L).shutDownWhenFull()),
            is(new FinalResultsSuppressionBuilder<>(null, maxRecords(2L).shutDownWhenFull())
            )
        );

        assertThat(
            untilWindowCloses(maxBytes(2L).shutDownWhenFull()),
            is(new FinalResultsSuppressionBuilder<>(null, maxBytes(2L).shutDownWhenFull())
            )
        );

        assertThat(
            untilWindowCloses(unbounded()).withName("name"),
            is(new FinalResultsSuppressionBuilder<>("name", unbounded()))
        );

        assertThat(
            untilWindowCloses(maxRecords(2L).shutDownWhenFull()).withName("name"),
            is(new FinalResultsSuppressionBuilder<>("name", maxRecords(2L).shutDownWhenFull())
            )
        );

        assertThat(
            untilWindowCloses(maxBytes(2L).shutDownWhenFull()).withName("name"),
            is(new FinalResultsSuppressionBuilder<>("name", maxBytes(2L).shutDownWhenFull())
            )
        );
    }
}

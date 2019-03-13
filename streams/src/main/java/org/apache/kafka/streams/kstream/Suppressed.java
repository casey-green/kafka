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

import java.time.Duration;
import java.util.Objects;

public interface Suppressed<K> {

    /**
     * Configures how the suppression operator buffers records.
     */
    final class BufferConfig {
        private final long maxRecords;
        private final long maxBytes;
        private final BufferFullStrategy bufferFullStrategy;

        BufferConfig(final long maxRecords, final long maxBytes, final BufferFullStrategy bufferFullStrategy) {
            this.maxRecords = maxRecords;
            this.maxBytes = maxBytes;
            this.bufferFullStrategy = bufferFullStrategy;
        }
        
        public long maxRecords() {
            return maxRecords;
        }
        
        public long maxBytes() {
            return maxBytes;
        }

        public BufferFullStrategy bufferFullStrategy() {
            return bufferFullStrategy;
        }

        /**
         * Create a size-constrained buffer in terms of the maximum number of keys it will store.
         */
        public static BufferConfig maxRecords(final long recordLimit) {
            return new BufferConfig(recordLimit, Long.MAX_VALUE, BufferFullStrategy.EMIT);
        }

        /**
         * Create a size-constrained buffer in terms of the maximum number of bytes it will use.
         */
        public static BufferConfig maxBytes(final long byteLimit) {
            return new BufferConfig(Long.MAX_VALUE, byteLimit, BufferFullStrategy.EMIT);
        }

        /**
         * Create a buffer unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        public static BufferConfig unbounded() {
            return new BufferConfig(Long.MAX_VALUE, Long.MAX_VALUE, BufferFullStrategy.SHUT_DOWN);
        }

        /**
         * Set a size constraint on the buffer in terms of the maximum number of keys it will store.
         */
        public BufferConfig withMaxRecords(final long recordLimit) {
            return new BufferConfig(recordLimit, maxBytes, bufferFullStrategy);
        }
        
        /**
         * Set a size constraint on the buffer, the maximum number of bytes it will use.
         */
        public BufferConfig withMaxBytes(final long byteLimit) {
            return new BufferConfig(maxRecords, byteLimit, bufferFullStrategy);
        }

        /**
         * Set the buffer to be unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        public BufferConfig withNoBound() {
            return unbounded();
        }

        /**
         * Set the buffer to gracefully shut down the application when any of its constraints are violated
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or shut down.
         * It will never emit early.
         */
        public BufferConfig shutDownWhenFull() {
            return new BufferConfig(maxRecords, maxBytes, BufferFullStrategy.SHUT_DOWN);
        }

        /**
         * Set the buffer to just emit the oldest records when any of its constraints are violated.
         *
         * This buffer is "not strict" in the sense that it may emit early, so it is suitable for reducing
         * duplicate results downstream, but does not promise to eliminate them.
         */
        public BufferConfig emitEarlyWhenFull() {
            return new BufferConfig(maxRecords, maxBytes, BufferFullStrategy.EMIT);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BufferConfig that = (BufferConfig) o;
            return maxRecords == that.maxRecords &&
                    maxBytes == that.maxBytes &&
                    bufferFullStrategy == that.bufferFullStrategy;
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxRecords, maxBytes, bufferFullStrategy);
        }

        @Override
        public String toString() {
            return "BufferConfig{maxRecords=" + maxRecords +
                    ", maxBytes=" + maxBytes +
                    ", bufferFullStrategy=" + bufferFullStrategy +
                    '}';
        }
    }

    /**
     * Configure the suppression to emit only the "final results" from the window.
     *
     * By default all Streams operators emit results whenever new results are available.
     * This includes windowed operations.
     *
     * This configuration will instead emit just one result per key for each window, guaranteeing
     * to deliver only the final result. This option is suitable for use cases in which the business logic
     * requires a hard guarantee that only the final result is propagated. For example, sending alerts.
     *
     * To accomplish this, the operator will buffer events from the window until the window close (that is,
     * until the end-time passes, and additionally until the grace period expires). Since windowed operators
     * are required to reject late events for a window whose grace period is expired, there is an additional
     * guarantee that the final results emitted from this suppression will match any queriable state upstream.
     *
     * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
     *                     This is required to be a "strict" config, since it would violate the "final results"
     *                     property to emit early and then issue an update later.
     * @return a "final results" mode suppression configuration
     */
    static Suppressed<Windowed> untilWindowCloses(final BufferConfig bufferConfig) {
        return new FinalResultsSuppressionBuilder<>(null, bufferConfig);
    }

    /**
     * Configure the suppression to wait {@code timeToWaitForMoreEvents} amount of time after receiving a record
     * before emitting it further downstream. If another record for the same key arrives in the mean time, it replaces
     * the first record in the buffer but does <em>not</em> re-start the timer.
     *
     * @param timeToWaitForMoreEvents The amount of time to wait, per record, for new events.
     * @param bufferConfig A configuration specifying how much space to use for buffering intermediate results.
     * @param <K> The key type for the KTable to apply this suppression to.
     * @return a suppression configuration
     */
    static <K> Suppressed<K> untilTimeLimit(final Duration timeToWaitForMoreEvents, final BufferConfig bufferConfig) {
        return new SuppressedInternal<>(null, timeToWaitForMoreEvents, bufferConfig, null, false);
    }

    /**
     * Use the specified name for the suppression node in the topology.
     * <p>
     * This can be used to insert a suppression without changing the rest of the topology names
     * (and therefore not requiring an application reset).
     * <p>
     * Note however, that once a suppression has buffered some records, removing it from the topology would cause
     * the loss of those records.
     * <p>
     * A suppression can be "disabled" with the configuration {@code untilTimeLimit(Duration.ZERO, ...}.
     *
     * @param name The name to be used for the suppression node and changelog topic
     * @return The same configuration with the addition of the given {@code name}.
     */
    Suppressed<K> withName(final String name);
}

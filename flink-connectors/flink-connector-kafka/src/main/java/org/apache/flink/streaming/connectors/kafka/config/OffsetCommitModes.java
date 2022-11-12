/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.annotation.Internal;

/** Utilities for {@link OffsetCommitMode}. */
@Internal
public class OffsetCommitModes {

    /**
     * Determine the offset commit mode using several configuration values.
     *
     * @param enableAutoCommit whether or not auto committing is enabled in the provided Kafka
     *     properties.
     * @param enableCommitOnCheckpoint whether or not committing on checkpoints is enabled.
     * @param enableCheckpointing whether or not checkpoint is enabled for the consumer.
     * @return the offset commit mode to use, based on the configuration values.
     */
    public static OffsetCommitMode fromConfiguration(
            boolean enableAutoCommit,
            boolean enableCommitOnCheckpoint,
            boolean enableCheckpointing) {

        /**
         * 是否开启checkpoint
         *      开启checkpoint：checkpoint完成时提交offset，如果没开启enableCommitOnCheckpoint就禁用提交(这个一般也没人会设置)
         *      没开启checkpoint：是否配置启用enable.auto.commit，启用则自动提交，否则禁用提交
         */
        if (enableCheckpointing) {
            // if checkpointing is enabled, the mode depends only on whether committing on
            // checkpoints is enabled
            return (enableCommitOnCheckpoint)
                    ? OffsetCommitMode.ON_CHECKPOINTS
                    : OffsetCommitMode.DISABLED;
        } else {
            // else, the mode depends only on whether auto committing is enabled in the provided
            // Kafka properties
            return (enableAutoCommit) ? OffsetCommitMode.KAFKA_PERIODIC : OffsetCommitMode.DISABLED;
        }
    }
}

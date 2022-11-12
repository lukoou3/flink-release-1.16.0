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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * Flink Sink将数据生成到Kafka主题中。接收器支持DeliveryGuarantee描述的所有交付保证(至少一次,精确一次)。
 *
 * DeliveryGuarantee.NONE不提供任何保证：如果Kafka代理出现问题，消息可能会丢失，如果Flink失败，消息可能重复。
 *
 * DeliveryGuarantee.AT_LEAST_ONCE接收器将等待Kafka缓冲区中的所有未完成记录被Kafka生产者在检查点上确认。
 * 如果Kafka brokers出现任何问题，则不会丢失任何消息，但当Flink重新启动时，消息可能会重复。
 *
 * DeliveryGuarantee.EXACTLY_ONCE：在这种模式下，KafkaSink将在一个Kafka事务中写入将在检查点提交给Kafka的所有消息。
 * 因此，如果使用者只读取提交的数据（请参阅Kafka使用者配置隔离级别），那么在Flink重新启动时不会看到重复数据。
 * 但是，这会有效地延迟记录写入，直到写入检查点，因此请相应地调整检查点持续时间。
 * 请确保在同一Kafka集群上运行的应用程序中使用唯一的transactionalIdPrefix，以便多个运行的作业不会干扰它们的事务！
 * 此外，强烈建议调整Kafka事务超时（链接）>>最大检查点持续时间+最大重新启动持续时间，否则当Kafka使未提交的事务过期时，可能会发生数据丢失。
 *
 *
 * Flink Sink to produce data into a Kafka topic. The sink supports all delivery guarantees
 * described by {@link DeliveryGuarantee}.
 * <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: messages may be lost in case
 *     of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} the sink will wait for all outstanding records in the
 *     Kafka buffers to be acknowledged by the Kafka producer on a checkpoint. No messages will be
 *     lost in case of any issue with the Kafka brokers but messages may be duplicated when Flink
 *     restarts.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: In this mode the KafkaSink will write all messages in
 *     a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer
 *     reads only committed data (see Kafka consumer config isolation.level), no duplicates will be
 *     seen in case of a Flink restart. However, this delays record writing effectively until a
 *     checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure that you
 *     use unique {@link #transactionalIdPrefix}s across your applications running on the same Kafka
 *     cluster such that multiple running jobs do not interfere in their transactions! Additionally,
 *     it is highly recommended to tweak Kafka transaction timeout (link) >> maximum checkpoint
 *     duration + maximum restart duration or data loss may happen when Kafka expires an uncommitted
 *     transaction.
 *
 * @param <IN> type of the records written to Kafka
 * @see KafkaSinkBuilder on how to construct a KafkaSink
 */
@PublicEvolving
public class KafkaSink<IN>
        implements StatefulSink<IN, KafkaWriterState>,
                TwoPhaseCommittingSink<IN, KafkaCommittable> {

    private final DeliveryGuarantee deliveryGuarantee;

    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;

    KafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.recordSerializer = recordSerializer;
    }

    /**
     * Create a {@link KafkaSinkBuilder} to construct a new {@link KafkaSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link KafkaSinkBuilder}
     */
    public static <IN> KafkaSinkBuilder<IN> builder() {
        return new KafkaSinkBuilder<>();
    }

    @Internal
    @Override
    public Committer<KafkaCommittable> createCommitter() throws IOException {
        return new KafkaCommitter(kafkaProducerConfig);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaCommittable> getCommittableSerializer() {
        return new KafkaCommittableSerializer();
    }

    @Internal
    @Override
    public KafkaWriter<IN> createWriter(InitContext context) throws IOException {
        return new KafkaWriter<IN>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                Collections.emptyList());
    }

    @Internal
    @Override
    public KafkaWriter<IN> restoreWriter(
            InitContext context, Collection<KafkaWriterState> recoveredState) throws IOException {
        return new KafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                recoveredState);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
        return new KafkaWriterStateSerializer();
    }

    @VisibleForTesting
    protected Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }
}

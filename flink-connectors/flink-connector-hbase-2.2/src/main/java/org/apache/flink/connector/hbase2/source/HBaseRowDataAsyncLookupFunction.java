/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase2.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseSerde;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The HBaseRowDataAsyncLookupFunction is an implementation to lookup HBase data by rowkey in async
 * fashion. It looks up the result as {@link RowData}.
 */
@Internal
public class HBaseRowDataAsyncLookupFunction extends AsyncLookupFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(HBaseRowDataAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String hTableName;
    private final byte[] serializedConfig;
    private final HBaseTableSchema hbaseTableSchema;
    private final String nullStringLiteral;

    private transient AsyncConnection asyncConnection;
    private transient AsyncTable<ScanResultConsumer> table;
    private transient HBaseSerde serde;

    private final int maxRetryTimes;

    /** The size for thread pool. */
    private static final int THREAD_POOL_SIZE = 16;

    public HBaseRowDataAsyncLookupFunction(
            Configuration configuration,
            String hTableName,
            HBaseTableSchema hbaseTableSchema,
            String nullStringLiteral,
            int maxRetryTimes) {
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(configuration);
        this.hTableName = hTableName;
        this.hbaseTableSchema = hbaseTableSchema;
        this.nullStringLiteral = nullStringLiteral;
        this.maxRetryTimes = maxRetryTimes;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        final ExecutorService threadPool =
                Executors.newFixedThreadPool(
                        THREAD_POOL_SIZE,
                        new ExecutorThreadFactory(
                                "hbase-async-lookup-worker", Threads.LOGGING_EXCEPTION_HANDLER));
        Configuration config = prepareRuntimeConfiguration();
        CompletableFuture<AsyncConnection> asyncConnectionFuture =
                ConnectionFactory.createAsyncConnection(config);
        try {
            asyncConnection = asyncConnectionFuture.get();
            table = asyncConnection.getTable(TableName.valueOf(hTableName), threadPool);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Exception while creating connection to HBase.", e);
            throw new RuntimeException("Cannot create connection to HBase.", e);
        }
        this.serde = new HBaseSerde(hbaseTableSchema, nullStringLiteral);
        LOG.info("end open.");
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keyRow A {@link RowData} that wraps lookup keys. Currently only support single rowkey.
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        int currentRetry = 0;
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        // fetch result
        fetchResult(future, currentRetry, ((GenericRowData) keyRow).getField(0));
        return future;
    }

    /**
     * Execute async fetch result .
     *
     * @param resultFuture The result or exception is returned.
     * @param currentRetry Current number of retries.
     * @param rowKey the lookup key.
     */
    private void fetchResult(
            CompletableFuture<Collection<RowData>> resultFuture, int currentRetry, Object rowKey) {
        Get get = serde.createGet(rowKey);
        CompletableFuture<Result> responseFuture = table.get(get);
        responseFuture.whenCompleteAsync(
                (result, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof TableNotFoundException) {
                            LOG.error("Table '{}' not found ", hTableName, throwable);
                            resultFuture.completeExceptionally(
                                    new RuntimeException(
                                            "HBase table '" + hTableName + "' not found.",
                                            throwable));
                        } else {
                            LOG.error(
                                    String.format(
                                            "HBase asyncLookup error, retry times = %d",
                                            currentRetry),
                                    throwable);
                            if (currentRetry >= maxRetryTimes) {
                                resultFuture.completeExceptionally(throwable);
                            } else {
                                try {
                                    Thread.sleep(1000 * currentRetry);
                                } catch (InterruptedException e1) {
                                    resultFuture.completeExceptionally(e1);
                                }
                                fetchResult(resultFuture, currentRetry + 1, rowKey);
                            }
                        }
                    } else {
                        if (result.isEmpty()) {
                            resultFuture.complete(Collections.emptyList());
                        } else {
                            resultFuture.complete(
                                    Collections.singletonList(serde.convertToNewRow(result)));
                        }
                    }
                });
    }

    private Configuration prepareRuntimeConfiguration() {
        // create default configuration from current runtime env (`hbase-site.xml` in classpath)
        // first,
        // and overwrite configuration using serialized configuration from client-side env
        // (`hbase-site.xml` in classpath).
        // user params from client-side have the highest priority
        Configuration runtimeConfig =
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());

        // do validation: check key option(s) in final runtime configuration
        if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
            LOG.error(
                    "can not connect to HBase without {} configuration",
                    HConstants.ZOOKEEPER_QUORUM);
            throw new IllegalArgumentException(
                    "check HBase configuration failed, lost: '"
                            + HConstants.ZOOKEEPER_QUORUM
                            + "'!");
        }

        return runtimeConfig;
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (null != table) {
            table = null;
        }
        if (null != asyncConnection) {
            try {
                asyncConnection.close();
                asyncConnection = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close connection", e);
            }
        }
        LOG.info("end close.");
    }

    @VisibleForTesting
    public String getHTableName() {
        return hTableName;
    }
}

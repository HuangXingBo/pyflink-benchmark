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

package com.pyflink.benchmark.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.Random;

public class SimpleDataGenSourceFunction extends RichParallelSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final int NUM = 1 << 10;

    private final long rowsPerSecond;
    private final long numberOfRows;
    private final long fieldLength;

    private transient String[] stringArrays;
    private transient Random random;

    private boolean isRunning = true;

    public SimpleDataGenSourceFunction(long rowsPerSecond, long numberOfRows, long fieldLength) {
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.fieldLength = fieldLength;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random(fieldLength);
        stringArrays = new String[NUM];
        for (int i = 0; i < NUM; i++) {
            stringArrays[i] = createNewJsonString((int) fieldLength);
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        long nextReadTime = System.currentTimeMillis();

        int outputSoFar = 0;
        while (isRunning) {
            for (int i = 0; i < rowsPerSecond; i++) {
                if (outputSoFar < numberOfRows) {
                    GenericRowData nextRow = new GenericRowData(2);
                    nextRow.setField(0, StringData.fromString(nextString()));
                    nextRow.setField(1, i);
                    outputSoFar++;
                    ctx.collect(nextRow);
                } else {
                    return;
                }
            }

            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis();
            while (toWaitMs > 0) {
                Thread.sleep(toWaitMs);
                toWaitMs = nextReadTime - System.currentTimeMillis();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private String nextString() {
        return stringArrays[random.nextInt(NUM)];
    }

    private String createNewJsonString(int length) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"a\":\"");
        for (int i = 0; i < length; i++) {
            sb.append((char) (65 + random.nextInt(26)));
        }
        sb.append("\"");
        sb.append("}");
        return sb.toString();
    }
}

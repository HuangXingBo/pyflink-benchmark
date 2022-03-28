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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class SimpleDataGenTableSource implements ScanTableSource {

    private final long rowsPerSecond;
    private final long numberOfRows;
    private final long fieldLength;

    public SimpleDataGenTableSource(long rowsPerSecond, long numberOfRows, long fieldLength) {
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.fieldLength = fieldLength;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        SimpleDataGenSourceFunction source =
                new SimpleDataGenSourceFunction(rowsPerSecond, numberOfRows, fieldLength);
        return SourceFunctionProvider.of(source, true);
    }

    @Override
    public SimpleDataGenTableSource copy() {
        return new SimpleDataGenTableSource(rowsPerSecond, numberOfRows, fieldLength);
    }

    @Override
    public String asSummaryString() {
        return "SimpleDataGenTableSource";
    }
}

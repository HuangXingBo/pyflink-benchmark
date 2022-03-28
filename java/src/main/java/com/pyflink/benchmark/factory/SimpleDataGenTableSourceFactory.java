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

package com.pyflink.benchmark.factory;

import com.pyflink.benchmark.source.SimpleDataGenTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class SimpleDataGenTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<Long> ROWS_PER_SECOND =
            key("rows-per-second")
                    .longType()
                    .defaultValue(100L)
                    .withDescription("Rows per second to control the emit rate.");

    public static final ConfigOption<Long> NUMBER_OF_ROWS =
            key("number-of-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Total number of rows to emit. By default, the source is unbounded.");

    public static final ConfigOption<Long> FIELD_LENGTH =
            key("field-length")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Total number of rows to emit. By default, the source is unbounded.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);
        return new SimpleDataGenTableSource(
                options.get(ROWS_PER_SECOND),
                options.get(NUMBER_OF_ROWS),
                options.get(FIELD_LENGTH));
    }

    @Override
    public String factoryIdentifier() {
        return "SimpleDataGen";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROWS_PER_SECOND);
        options.add(NUMBER_OF_ROWS);
        options.add(FIELD_LENGTH);
        return options;
    }
}

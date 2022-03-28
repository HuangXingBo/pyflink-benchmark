package com.pyflink.benchmark.function;

import org.apache.flink.table.functions.ScalarFunction;

public class StringUpper extends ScalarFunction {
    public String eval(String id) {
        return id.toUpperCase();
    }
}

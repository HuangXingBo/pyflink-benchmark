package com.pyflink.benchmark.function;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class JsonValueLower extends ScalarFunction {
    private transient ObjectMapper mapper;
    private transient ObjectWriter writer;

    @Override
    public void open(FunctionContext context) throws Exception {
        this.mapper = new ObjectMapper();
        this.writer = mapper.writerWithDefaultPrettyPrinter();
    }

    public String eval(String s) {
        try {
            StringObject object = mapper.readValue(s, StringObject.class);
            object.setA(object.a.toLowerCase());
            return writer.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to read json value", e);
        }
    }

    private static class StringObject {
        private String a;

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        @Override
        public String toString() {
            return "StringObject{" + "a='" + a + '\'' + '}';
        }
    }
}

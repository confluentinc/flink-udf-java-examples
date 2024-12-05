/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.udf.examples.scalar;

import org.apache.flink.table.functions.ScalarFunction;

/** A simple scalar function that concatenates two strings. */
public class ConcatScalarFunction extends ScalarFunction {
    public String eval(String a, String b) {
        return a + b;
    }
}

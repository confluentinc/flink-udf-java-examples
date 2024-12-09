/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.udf.examples.scalar;

import org.apache.flink.table.functions.ScalarFunction;

/** A simple scalar function that adds two integers. */
public class SumScalarFunction extends ScalarFunction {

    public int eval(int a, int b) {
        return a + b;
    }
}

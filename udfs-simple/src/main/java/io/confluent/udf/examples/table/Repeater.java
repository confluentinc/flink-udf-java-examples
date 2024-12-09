/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.udf.examples.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** A simple table function that repeats a number a given number of times. */
@FunctionHint(output = @DataTypeHint("ROW<val INT>"))
public class Repeater extends TableFunction<Row> {

    public void eval(Integer number, Integer times) {
        for (int i = 0; i < times; i++) {
            // use collect(...) to emit a row
            collect(Row.of(number));
        }
    }
}

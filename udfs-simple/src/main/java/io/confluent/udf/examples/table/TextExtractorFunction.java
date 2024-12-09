/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.udf.examples.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** A simple table function that extracts words from a text. */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class TextExtractorFunction extends TableFunction<Row> {

    public void eval(String str) {
        for (String s : str.split("[^0-9a-zA-Z]+")) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }
}

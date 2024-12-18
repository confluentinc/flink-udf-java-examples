package io.confluent.udf.examples.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/** A simple table function that splits a string into words using a delimiter. */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class Splitter extends TableFunction<Row> {

    public void eval(String str, String delimiter) {
        for (String s : str.split(delimiter)) {
            // use collect(...) to emit a row
            collect(Row.of(s));
        }
    }
}

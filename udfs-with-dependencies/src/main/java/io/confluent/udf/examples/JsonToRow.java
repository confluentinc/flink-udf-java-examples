package io.confluent.udf.examples;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@FunctionHint(output = @DataTypeHint("ROW<field STRING, value STRING>"))
public class JsonToRow extends TableFunction<Row> {

    public void eval(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(json);
            if (node.isObject()) {
                node.fields()
                        .forEachRemaining(
                                field -> {
                                    collect(Row.of(field.getKey(), field.getValue().asText()));
                                });
            }
        } catch (JsonProcessingException e) {
            // Do nothing for now!
        }
    }
}

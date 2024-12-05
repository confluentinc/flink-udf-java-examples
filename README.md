# Apache Flink® User Defined Functions on Confluent Cloud - Examples

This repository contains examples for running Apache Flink's UDFs on Confluent Cloud.

## Introduction to User Defined Functions (UDFs) in Java

[UDFs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/udfs/)
are a way to extend the functionality of Flink SQL and invoke them in similar ways to built-in functions.

There are two types of UDFs supported in Confluent Cloud:
- Scalar Functions: Map scalar values to a new scalar value.
- Table Functions: Map scalar values to new rows.

## Example Use
A scalar function can be used as part of a clause expecting a single value, for example, as a projection.

```SQL
SELECT price + CUSTOM_TAX_FUNCTION(location) FROM (VALUES ('USA', 100)) AS Sales(location, price);
```

To define a scalar function, you need only to extend `ScalarFunction` as below:
```java
public class CustomTax extends ScalarFunction {
    public Integer eval(String location) {
        if (location.equals("USA")) {
            return 10;
        } if (location.equals("EU")) {
            return 5;
        } else {
            return 0;
        }
    }
}
```

Table functions can be invoked using a `LATERAL TABLE` clause, for example, to explode a list of values.

```SQL
SELECT res FROM (VALUES ARRAY[1, 2, 3], ARRAY[4, 5]) AS T1(arr), LATERAL TABLE(EXPLODE(arr)) AS T2(res);
```

We can then define a table function by extending `TableFunction`:

```java
public class Explode extends TableFunction<Integer> {
    public void eval(List<Integer> arr) {
        for (Integer i : arr) {
            collect(i);
        }
    }
}
```

## Building the examples

```shell
./mvnw clean package
```

This should produce a jar for each of the modules, which can be uploaded to Confluent Cloud.

For example:
```shell
> find . -wholename '*/target/*jar' | grep -v original
./udfs-simple/target/udfs-simple-1.0.0.jar
./udfs-with-dependencies/target/udfs-with-dependencies-1.0.0.jar
```
You should then upload the jars to Confluent Cloud, and create a UDF in your catalog.
More details on how to do that can be found 
[here](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-udf.html).
package io.confluent.udf.examples.log;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;

/* This class is a SumScalar function that logs messages at different levels */
public class LogSumScalarFunction extends ScalarFunction {

    private static final Logger LOGGER = LogManager.getLogger();

    public int eval(int a, int b) {
        String value = String.format("SumScalar of %d and %d", a, b);
        Date now = new Date();

        // You can choose the logging level for log messages.
        LOGGER.info(value + " info log messages by log4j logger --- " + now);
        LOGGER.error(value + " error log messages by log4j logger --- " + now);
        LOGGER.warn(value + " warn log messages by log4j logger --- " + now);
        LOGGER.debug(value + " debug log messages by log4j logger --- " + now);
        return a + b;
    }
}

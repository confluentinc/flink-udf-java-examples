/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.udf.examples.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;

/** Generates a random string of the given length. */
public class RandomStringGenerator extends ScalarFunction {

    private static final Random RANDOM = new Random();
    private static final String CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public String eval(int bytes) {
        byte[] bytesArray = new byte[bytes];
        for (int i = 0; i < bytes; i++) {
            bytesArray[i] = (byte) CHARS.charAt(RANDOM.nextInt(CHARS.length()));
        }
        return new String(bytesArray);
    }
}

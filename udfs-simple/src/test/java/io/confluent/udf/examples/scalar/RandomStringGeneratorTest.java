package io.confluent.udf.examples.scalar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RandomStringGenerator}. */
public class RandomStringGeneratorTest {

    private RandomStringGenerator udf = new RandomStringGenerator();

    @BeforeEach
    public void before() {}

    @Test
    public void testRandom() {
        assertThat(udf.eval(10)).hasSize(10);
        assertThat(udf.eval(20)).hasSize(20);
    }
}

package com.syonekura;

import org.junit.Test;
import org.springframework.batch.item.ItemReader;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by syonekura on 20-04-17.
 *
 * Tests para {@link FakeReader}
 */
public class FakeReaderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor1() throws Exception {
        new FakeReader(-1, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor2() throws Exception {
        new FakeReader(10, -1);
    }

    @Test
    public void testRead() throws Exception {
        int recordsToFake = 10;
        int recordsLength = 5;
        ItemReader<String> itemReader = new FakeReader(recordsToFake, recordsLength);
        for (int i = 0; i < recordsToFake; i++) {
            String item = itemReader.read();
            assertThat(item, is(notNullValue()));
            assertThat(item.length(), is(recordsLength));
        }
        assertThat(itemReader.read(), is(nullValue()));
    }
}
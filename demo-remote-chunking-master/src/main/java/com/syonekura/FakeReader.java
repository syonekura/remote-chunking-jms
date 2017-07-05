package com.syonekura;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

/**
 * Created by syonekura on 19-04-17.
 *
 * Implementación básica de un {@link ItemReader} que genera instancias aleatorias de {@link String} de un largo dado
 */
@Slf4j
public class FakeReader implements ItemReader<String> {
    private int recordsToFake;
    private int recordsLength;

    FakeReader(int recordsToFake, int recordsLength) {
        Assert.isTrue(recordsToFake > 0, "Cantidad de registros debe ser mayor a 0");
        this.recordsToFake = recordsToFake;
        Assert.isTrue(recordsLength > 0, "Largo de registro debe ser mayor a 0");
        this.recordsLength = recordsLength;
    }

    @Override
    public synchronized String read() throws Exception {
        if (recordsToFake > 0){
            recordsToFake--;
            String result = RandomStringUtils.randomAlphanumeric(recordsLength);
            log.debug("Remaining {}, str:{}", recordsToFake, result);
            return result;
        }
        return null;
    }
}

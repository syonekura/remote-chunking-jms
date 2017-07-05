package com.syonekura;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * Created by syonekura on 19-04-17.
 */
@Slf4j
@Component
public class SimpleStringProcessor implements ItemProcessor<String, String> {
    @Override
    public String process(String s) throws Exception {
        log.info("Procesando {}", s);
        return "//" + s.replaceAll("[A-Za-z]", " ") + "//";
    }
}

package org.exam.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Unit test for simple App.
 */
@Slf4j
public class MainTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldGenerateOutput() {
        log.info("starting...");
        String[] testArgs = new String[]{};
        Main.main(testArgs);
        assertThat(Files.exists(Paths.get(ConfigFactory
                .load("application.conf")
                .getString("app.data.output"))))
                .isTrue();
    }
}

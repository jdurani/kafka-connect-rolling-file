package org.jdurani.rollingfile.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RollingFileSourceConnectorTest {

    private RollingFileSourceConnector test;

    @BeforeEach
    void setUp() {
        test = new RollingFileSourceConnector();
    }

    @Test
    void taskClass() {
        Assertions.assertSame(test.taskClass(), RollingFileSourceTask.class);
    }

    @Test
    void taskConfigs() {
        Map<String, String> c = new HashMap<>();
        String batch = "10";
        String dir = "xxx";
        c.put(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG, batch);
        c.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, dir);
        c.put(RollingFileConfig.ROLLING_FILE_IGNORE_PARTITION_CONFIG, "true");
        c.put(RollingFileConfig.ROLLING_FILE_IGNORE_TIMESTAMP_CONFIG, "true");
        test.start(c);
        List<Map<String, String>> cfgs = test.taskConfigs(100);
        Assertions.assertNotNull(cfgs);
        Assertions.assertEquals(1, cfgs.size());
        Map<String, String> cfg = cfgs.get(0);
        Assertions.assertNotNull(cfg);
        Assertions.assertEquals(4, cfg.size());
        Assertions.assertAll(
                () -> Assertions.assertEquals(dir, cfg.get(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG)),
                () -> Assertions.assertEquals("true", cfg.get(RollingFileConfig.ROLLING_FILE_IGNORE_PARTITION_CONFIG)),
                () -> Assertions.assertEquals("true", cfg.get(RollingFileConfig.ROLLING_FILE_IGNORE_TIMESTAMP_CONFIG)),
                () -> Assertions.assertEquals(batch, cfg.get(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG)));
    }

    @Test
    void taskConfigs2() {
        Map<String, String> c = new HashMap<>();
        String batch = "10";
        String dir = "xxx";
        c.put(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG, batch);
        c.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, dir);
        c.put(RollingFileConfig.ROLLING_FILE_IGNORE_PARTITION_CONFIG, "false");
        c.put(RollingFileConfig.ROLLING_FILE_IGNORE_TIMESTAMP_CONFIG, "false");
        test.start(c);
        List<Map<String, String>> cfgs = test.taskConfigs(100);
        Assertions.assertNotNull(cfgs);
        Assertions.assertEquals(1, cfgs.size());
        Map<String, String> cfg = cfgs.get(0);
        Assertions.assertNotNull(cfg);
        Assertions.assertEquals(4, cfg.size());
        Assertions.assertAll(
                () -> Assertions.assertEquals(dir, cfg.get(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG)),
                () -> Assertions.assertEquals("false", cfg.get(RollingFileConfig.ROLLING_FILE_IGNORE_PARTITION_CONFIG)),
                () -> Assertions.assertEquals("false", cfg.get(RollingFileConfig.ROLLING_FILE_IGNORE_TIMESTAMP_CONFIG)),
                () -> Assertions.assertEquals(batch, cfg.get(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG)));
    }

    @Test
    void config() {
        Assertions.assertSame(RollingFileConfig.sourceConfig(), test.config());
    }

    @Test
    void version() {
        Assertions.assertSame(VersionHolder.VERSION, test.version());
    }
}

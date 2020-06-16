package org.jdurani.rollingfile.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RollingFileSinkConnectorTest {

    private RollingFileSinkConnector test;

    @BeforeEach
    void setUp() {
        test = new RollingFileSinkConnector();
    }

    @Test
    void taskClass() {
        Assertions.assertSame(RollingFileSinkTask.class, test.taskClass());
    }

    @Test
    void taskConfigs() {
        String dir = "dir";
        int count = 1;
        int ms = 10;
        Map<String, String> m = new HashMap<>();
        m.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, dir);
        m.put(RollingFileConfig.ROLLING_FILE_FLUSH_COUNT_CONFIG, String.valueOf(count));
        m.put(RollingFileConfig.ROLLING_FILE_FLUSH_MS_CONFIG, String.valueOf(ms));
        test.start(m);
        List<Map<String, String>> maps = test.taskConfigs(3);
        Assertions.assertAll(
                () -> Assertions.assertNotNull(maps),
                () -> Assertions.assertEquals(3, maps.size()));
        Assertions.assertAll(
                () -> Assertions.assertNotNull(maps.get(0)),
                () -> Assertions.assertNotNull(maps.get(1)),
                () -> Assertions.assertNotNull(maps.get(2)));
        Assertions.assertEquals(maps.get(0), maps.get(1));
        Assertions.assertEquals(maps.get(0), maps.get(2));
        Assertions.assertEquals(maps.get(1), maps.get(2));
        Map<String, String> cfg = maps.get(0);
        Assertions.assertEquals(3, cfg.size());
        Assertions.assertEquals(dir, cfg.get(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG));
        Assertions.assertEquals(Integer.toString(count), cfg.get(RollingFileConfig.ROLLING_FILE_FLUSH_COUNT_CONFIG));
        Assertions.assertEquals(Integer.toString(ms), cfg.get(RollingFileConfig.ROLLING_FILE_FLUSH_MS_CONFIG));
    }

    @Test
    void config() {
        Assertions.assertSame(RollingFileConfig.sinkConfig(), test.config());
    }

    @Test
    void version() {
        Assertions.assertSame(VersionHolder.VERSION, test.version());
    }
}

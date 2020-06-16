package org.jdurani.rollingfile.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;

/**
 * Rolling file sink connector
 */
public class RollingFileSinkConnector extends SinkConnector {

    private String dir;
    private long flushCount;
    private long flushMs;

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig c = new AbstractConfig(config(), props);
        dir = c.getString(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG);
        flushCount = c.getLong(RollingFileConfig.ROLLING_FILE_FLUSH_COUNT_CONFIG);
        flushMs = c.getLong(RollingFileConfig.ROLLING_FILE_FLUSH_MS_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RollingFileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> c = new HashMap<>();
            c.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, dir);
            c.put(RollingFileConfig.ROLLING_FILE_FLUSH_COUNT_CONFIG, Long.toString(flushCount));
            c.put(RollingFileConfig.ROLLING_FILE_FLUSH_MS_CONFIG, Long.toString(flushMs));
            configs.add(c);
        }
        return configs;
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public ConfigDef config() {
        return RollingFileConfig.sinkConfig();
    }

    @Override
    public String version() {
        return VersionHolder.VERSION;
    }
}

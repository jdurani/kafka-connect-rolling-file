package org.jdurani.rollingfile.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;

/**
 * Source connector.
 */
public class RollingFileSourceConnector extends SourceConnector {

    private String dir;
    private int batchSize;

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig c = new AbstractConfig(config(), props);
        dir = c.getString(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG);
        batchSize = c.getInt(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RollingFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> cfgs = new ArrayList<>(1);
        Map<String, String> cfg = new HashMap<>(2);
        cfg.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, dir);
        cfg.put(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        cfgs.add(cfg);
        return cfgs;
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public ConfigDef config() {
        return RollingFileConfig.sourceConfig();
    }

    @Override
    public String version() {
        return VersionHolder.VERSION;
    }
}

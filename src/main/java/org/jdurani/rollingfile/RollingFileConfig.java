package org.jdurani.rollingfile;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Connector configs.
 */
public class RollingFileConfig {

    public static final String ROLLING_FILE_DIRECTORY_CONFIG = "rolling.file.directory";
    private static final String ROLLING_FILE_DIRECTORY_IN_DOC = "Input directory to load files from.";
    private static final String ROLLING_FILE_DIRECTORY_OUT_DOC = "Output directory to store files to.";

    public static final String ROLLING_FILE_FLUSH_COUNT_CONFIG = "rolling.file.flush.count";
    private static final String ROLLING_FILE_FLUSH_COUNT_DOC = "Number of written records before rolling to next file.";

    public static final String ROLLING_FILE_FLUSH_MS_CONFIG = "rolling.file.flush.ms";
    private static final String ROLLING_FILE_FLUSH_MS_DOC = "How long wait before rolling to next file. This is rather fixed"
            + " rate at which to roll file than time between two consequent rolls. I.e. roll of a file may happen before"
            + " flush time elapses since last roll which was triggered by '" + ROLLING_FILE_FLUSH_COUNT_CONFIG + "'";

    public static final String ROLLING_FILE_BATCH_SIZE_CONFIG = "rolling.file.batch.size";
    private static final String BATCH_SIZE_DOC = "Maximum number of records to load from files in one batch.";

    private static final ConfigDef SINK = new ConfigDef()
            .define(ROLLING_FILE_DIRECTORY_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    ROLLING_FILE_DIRECTORY_OUT_DOC)
            .define(ROLLING_FILE_FLUSH_COUNT_CONFIG,
                    ConfigDef.Type.LONG,
                    100_000L,
                    ConfigDef.Range.atLeast(1L),
                    ConfigDef.Importance.MEDIUM,
                    ROLLING_FILE_FLUSH_COUNT_DOC)
            .define(ROLLING_FILE_FLUSH_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    10_000L,
                    ConfigDef.Range.atLeast(1L),
                    ConfigDef.Importance.MEDIUM,
                    ROLLING_FILE_FLUSH_MS_DOC);
    private static final ConfigDef SOURCE = new ConfigDef()
            .define(ROLLING_FILE_DIRECTORY_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    ROLLING_FILE_DIRECTORY_IN_DOC)
            .define(ROLLING_FILE_BATCH_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    10_000,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.MEDIUM,
                    BATCH_SIZE_DOC);

    /**
     * @return sink config
     */
    public static ConfigDef sinkConfig() {
        return SINK;
    }

    /**
     * @return source config
     */
    public static ConfigDef sourceConfig() {
        return SOURCE;
    }
}

package org.jdurani.rollingfile.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.VersionHolder;
import org.jdurani.rollingfile.exception.CloseException;
import org.jdurani.rollingfile.exception.FlushException;
import org.jdurani.rollingfile.exception.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink task to create rolling files.
 */
public class RollingFileSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileSinkTask.class);

    private String dir;
    private long flushCount;
    private long flushMs;

    private final Map<TopicPartition, RollingFileWriter> writeInfoMap = new HashMap<>();

    @Override
    public String version() {
        return VersionHolder.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        dir = props.get(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG);
        flushCount = Long.parseLong(props.get(RollingFileConfig.ROLLING_FILE_FLUSH_COUNT_CONFIG));
        flushMs = Long.parseLong(props.get(RollingFileConfig.ROLLING_FILE_FLUSH_MS_CONFIG));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        try {
            for (SinkRecord r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
                writeInfoMap.computeIfAbsent(tp, this::getWriter)
                        .write(r);
            }
        } catch (IOException e) {
            throw new WriteException("Error writing data", e);
        }
    }

    /**
     * This method exists for better testing and mocking.
     *
     * @param tp topic-partition
     *
     * @return writer
     */
    RollingFileWriter getWriter(TopicPartition tp) {
        return new RollingFileWriter(tp, dir, flushCount, flushMs);
    }

    @Override
    public void stop() {
        close(null);
    }

    @Override
    protected void finalize() {
        stop();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        AtomicBoolean thrown = new AtomicBoolean(false);
        writeInfoMap.forEach((k, v) -> {
            try {
                v.flush();
            } catch (IOException e) {
                LOG.error("Error flushing data", e);
                thrown.set(true);
            }
        });
        if (thrown.get()) {
            throw new FlushException("Error flushing files. See previous log.");
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // no-op
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        Collection<TopicPartition> toClose = partitions == null ? new HashSet<>(writeInfoMap.keySet()) : partitions;
        AtomicBoolean thrown = new AtomicBoolean(false);
        toClose.forEach(key -> {
            RollingFileWriter item = writeInfoMap.remove(key);
            if (item != null) {
                try {
                    item.flush();
                    item.close();
                } catch (IOException e) {
                    LOG.error("Error flushing and closing data", e);
                    thrown.set(true);
                }
            }
        });
        if (thrown.get()) {
            throw new CloseException("Error closing files. See previous log.");
        }
    }
}

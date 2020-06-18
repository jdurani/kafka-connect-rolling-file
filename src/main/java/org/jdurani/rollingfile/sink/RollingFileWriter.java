package org.jdurani.rollingfile.sink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record writer.
 */
public class RollingFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);

    public static final String FILE_NAME_FORMAT = "%1$010d-%2$019d.txt";
    public static final String KEY_VALUE_SEPARATOR = " ";
    public static final byte[] RECORD_SEPARATOR = {'\n'};

    private static final byte[] KEY_VALUE_SEPARATOR_BYTES = KEY_VALUE_SEPARATOR.getBytes();
    private static final byte[] EMPTY_BYTES = Base64.getEncoder().encode(new byte[0]);

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });
    private final TopicPartition tp;
    private final String dir;
    private final long flushCount;
    private final long flushMs;
    private long writtenLines;
    private long lastFileRoll;
    private OutputStream os;
    private File targetFile;
    private File openedFile;
    private boolean destroyed = false;

    /**
     * New instance.
     *
     * @param tp topic partition for writing
     * @param dir base directory to write data
     * @param flushCount flush count
     * @param flushMs flush time
     */
    public RollingFileWriter(TopicPartition tp, String dir, long flushCount, long flushMs) {
        this.tp = tp;
        this.dir = dir;
        this.flushCount = flushCount;
        this.flushMs = flushMs;
        executor.scheduleWithFixedDelay(() -> {
            try {
                rollIfNeeded();
            } catch (IOException e) {
                LOG.error("Error while flushing data", e);
            }
        }, this.flushMs, this.flushMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Flush bytes to underlying output stream.
     *
     * @throws IOException in case of error
     */
    public synchronized void flush() throws IOException {
        if (os != null) {
            os.flush();
        }
    }

    /**
     * Close underlying output stream and rename temporal file to target file.
     *
     * @throws IOException in case of error
     */
    public synchronized void close() throws IOException {
        if (os != null) {
            os.flush();
            os.close();
            LOG.debug("Closing and renaming file {} -> {}", openedFile.getAbsolutePath(), targetFile.getAbsolutePath());
            if (!openedFile.renameTo(targetFile)) {
                throw new IOException("File " + openedFile.getAbsolutePath() + " not moved to " + targetFile.getAbsolutePath());
            }
            os = null;
            openedFile = null;
            targetFile = null;
            writtenLines = 0L;
            lastFileRoll = System.currentTimeMillis();
        }
    }

    /**
     * Write data to underlying output stream.
     *
     * @param r data to write
     *
     * @throws IOException in case of error
     * @throws IllegalStateException if record does not belong to our topic-partition or writer is destroyed
     */
    public synchronized void write(SinkRecord r) throws IOException, IllegalStateException {
        if (destroyed) {
            throw new IllegalStateException("Writer destroyed.");
        }
        validateTopicPartition(r);
        openIfNeeded(r.kafkaOffset());
        os.write(toBase64(r.key()));
        os.write(KEY_VALUE_SEPARATOR_BYTES);
        os.write(toBase64(r.value()));
        os.write(RECORD_SEPARATOR);
        writtenLines++;
        rollIfNeeded();
    }

    /**
     * Validate record.
     *
     * @param r record to validate
     *
     * @throws IllegalStateException if record does not belong to our topic-partition
     * @see #write(SinkRecord)
     */
    private void validateTopicPartition(SinkRecord r) throws IllegalStateException {
        if (!Objects.equals(r.topic(), tp.topic()) ||
                !Objects.equals(r.kafkaPartition(), tp.partition())) {
            throw new IllegalStateException("Sink record does not belong to this topic-partition '" + tp + "' - " + r);
        }
    }

    /**
     * @param value value to encode as base64
     *
     * @return encoded bytes or {@link #EMPTY_BYTES} if vlaue is {@code null}
     *
     * @see #toBytes(Object)
     */
    private byte[] toBase64(Object value) {
        if (value == null) {
            return EMPTY_BYTES;
        }
        return Base64.getEncoder().encode(toBytes(value));
    }

    /**
     * Converts value to bytes. Supported are only {@code byte[]} and {@code String}.
     *
     * @param value value to get bytes from
     *
     * @return bytes
     *
     * @see #toBase64(Object)
     */
    private byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Unsupported object to write - [class] " + value.getClass());
    }

    /**
     * Close current stream if enough lines has been written or flush time has elapsed.
     *
     * @throws IOException on case of error
     * @see #close()
     * @see #openIfNeeded(long)
     */
    private synchronized void rollIfNeeded() throws IOException {
        long sinceLastRoll = System.currentTimeMillis() - lastFileRoll;
        if (writtenLines > 0
                && (writtenLines >= flushCount || sinceLastRoll >= flushMs)) {
            LOG.debug("Rolling file - written lines: {}, ms since last flush: {}", writtenLines, sinceLastRoll);
            close();
        }
    }

    /**
     * @param offset offset currently being written
     *
     * @throws IOException in case of error
     * @see #close()
     * @see #rollIfNeeded()
     */
    private synchronized void openIfNeeded(long offset) throws IOException {
        if (os == null) {
            File target = Paths
                    .get(dir,
                            tp.topic(),
                            String.format(FILE_NAME_FORMAT, tp.partition(), offset))
                    .toFile();
            File parent = target.getParentFile();
            File opened = new File(parent, ".tmp-" + target.getName() + ".tmp");
            LOG.debug("Opening new file [tmp: {}] - {}", opened.getAbsolutePath(), target.getAbsolutePath());
            if (!parent.exists() && !parent.mkdirs()) {
                throw new IOException("Cannot create parent directory " + parent.getAbsolutePath());
            }
            os = new FileOutputStream(opened, false); // we are writing same offsets again - we can override file
            openedFile = opened;
            targetFile = target;
            lastFileRoll = System.currentTimeMillis();
        }
    }

    /**
     * Close and destroy this instance.
     *
     * @throws IOException in case of error
     * @see #close()
     */
    public synchronized void destroy() throws IOException {
        close();
        executor.shutdown();
        destroyed = true;
    }
}

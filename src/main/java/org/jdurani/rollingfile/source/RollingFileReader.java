package org.jdurani.rollingfile.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.jdurani.rollingfile.exception.ReadException;
import org.jdurani.rollingfile.sink.RollingFileWriter;

/**
 * Reader of rolling files.
 */
public class RollingFileReader {

    static final String FILE_NAME_KEY = "file";
    static final String LINES_READ_OFFSETS = "lines_read";
    static final String CHARS_READ_OFFSETS = "chars_read";

    private final String topic;
    private final BufferedReader reader;
    private final String fileAbsolutePath;
    private final Map<String, String> sourcePartition;

    private long linesRead;
    private long charsRead;

    /**
     * @param data data file to read
     * @param topic topic for data
     * @param osr offset storage reader to get stored (already read) offsets
     *
     * @throws IOException in case there is error while seeking to correct position in file
     */
    public RollingFileReader(File data, String topic, OffsetStorageReader osr) throws IOException {
        this.fileAbsolutePath = data.getAbsolutePath();
        this.topic = topic;
        reader = new BufferedReader(new FileReader(data));
        sourcePartition = Collections.singletonMap(FILE_NAME_KEY, this.fileAbsolutePath);
        Map<String, Object> offset = osr.offset(sourcePartition);
        linesRead = 0L;
        charsRead = 0L;
        if (offset != null && !offset.isEmpty()) {
            linesRead = getLong(offset, LINES_READ_OFFSETS);
            charsRead = getLong(offset, CHARS_READ_OFFSETS);
        }
        long skipped = reader.skip(charsRead);
        if (skipped != charsRead) {
            throw new IllegalStateException("Expected to skip " + charsRead + " characters, but actually skipped " + skipped + " [" + data.getAbsolutePath() + "]");
        }
    }

    /**
     * @param map map
     * @param key key
     *
     * @return value as long
     */
    private static long getLong(Map<String, Object> map, String key) {
        Object o = map.get(key);
        if (o != null) {
            if (o instanceof Number) {
                return ((Number) o).longValue();
            } else {
                return Long.parseLong(o.toString());
            }
        }
        return 0L;
    }

    public String getFileAbsolutePath() {
        return fileAbsolutePath;
    }

    /**
     * @return next record read from file
     *
     * @throws IOException in case of error while reading data
     */
    public SourceRecord nextRecord() throws IOException {
        String s = reader.readLine();
        if (s == null) {
            return null;
        }
        int idx = s.indexOf(RollingFileWriter.KEY_VALUE_SEPARATOR);
        if (idx < 0) {
            throw new ReadException("Wrong line format - [file=" + fileAbsolutePath + ", line=" + (linesRead + 1) + "] " + s);
        }
        Base64.Decoder dec = Base64.getDecoder();
        byte[] key = dec.decode(s.substring(0, idx));
        byte[] value = dec.decode(s.substring(idx + RollingFileWriter.KEY_VALUE_SEPARATOR.length()));
        Map<String, Long> sourceOffset = new HashMap<>();
        sourceOffset.put(LINES_READ_OFFSETS, ++linesRead);
        charsRead += s.length() + RollingFileWriter.RECORD_SEPARATOR.length;
        sourceOffset.put(CHARS_READ_OFFSETS, charsRead);
        return new SourceRecord(sourcePartition, sourceOffset, topic,
                Schema.BYTES_SCHEMA, key,
                Schema.BYTES_SCHEMA, value);
    }

    /**
     * Close underlying input reader.
     *
     * @throws IOException in case of error
     */
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}

package org.jdurani.rollingfile.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.jdurani.rollingfile.Utils;
import org.jdurani.rollingfile.sink.RollingFileWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RollingFileReaderTest {

    private static int idx = 0;
    private RollingFileReader test;
    private static File base;

    @BeforeAll
    static void beforeAll() throws IOException {
        base = Utils.tmpDir(false).toFile();
    }

    @BeforeEach
    void init() {
        test = null;
    }

    @Test
    void emptyFile() throws IOException {
        initTest(0, 0);
        Assertions.assertNull(test.nextRecord(false));
    }

    @Test
    void readFull() throws IOException {
        String key1 = "aa\u010Dxx";
        String l1 = l(key1, "bff5");
        String l2 = l("c", "d");
        Map<String, ?> sp = initTest(0, 0, l1, l2);
        SourceRecord r1 = test.nextRecord(false);
        SourceRecord r2 = test.nextRecord(false);
        Assertions.assertAll(
                () -> Assertions.assertNotNull(r1),
                () -> Assertions.assertNotNull(r2),
                () -> Assertions.assertNull(test.nextRecord(false)));
        Assertions.assertAll(
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.keySchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.valueSchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r2.keySchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r2.valueSchema()),
                () -> Assertions.assertNull(r1.kafkaPartition()),
                () -> Assertions.assertNull(r2.kafkaPartition()),
                () -> Assertions.assertNull(r1.timestamp()),
                () -> Assertions.assertNull(r2.timestamp()),
                () -> Assertions.assertEquals(sp, r1.sourcePartition()),
                () -> Assertions.assertEquals(sp, r2.sourcePartition()),
                () -> Assertions.assertArrayEquals(key1.getBytes(), (byte[]) r1.key()),
                () -> Assertions.assertArrayEquals(new byte[] {'b', 'f', 'f', '5'}, (byte[]) r1.value()),
                () -> Assertions.assertArrayEquals(new byte[] {'c'}, (byte[]) r2.key()),
                () -> Assertions.assertArrayEquals(new byte[] {'d'}, (byte[]) r2.value()));
        Map<String, ?> so1 = r1.sourceOffset();
        Map<String, ?> so2 = r2.sourceOffset();
        Assertions.assertAll(
                () -> Assertions.assertNotNull(so1),
                () -> Assertions.assertNotNull(so2));
        Assertions.assertAll(
                () -> Assertions.assertEquals(1L, so1.get(RollingFileReader.LINES_READ_OFFSETS)),
                () -> Assertions.assertEquals(l1.length() + (long) RollingFileWriter.RECORD_SEPARATOR.length, so1.get(RollingFileReader.CHARS_READ_OFFSETS)),
                () -> Assertions.assertEquals(2L, so2.get(RollingFileReader.LINES_READ_OFFSETS)),
                () -> Assertions.assertEquals(l1.length() + l2.length() + 2L * (long) RollingFileWriter.RECORD_SEPARATOR.length, so2.get(RollingFileReader.CHARS_READ_OFFSETS)));
    }

    @Test
    void readTimestamps() throws IOException {
        String l1 = l(100L, "a", "b");
        Map<String, ?> sp = initTest(0, 0, l1);
        SourceRecord r1 = test.nextRecord(false);
        Assertions.assertAll(
                () -> Assertions.assertNotNull(r1),
                () -> Assertions.assertNull(test.nextRecord(false)));
        Assertions.assertAll(
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.keySchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.valueSchema()),
                () -> Assertions.assertNull(r1.kafkaPartition()),
                () -> Assertions.assertEquals(100L, r1.timestamp()),
                () -> Assertions.assertEquals(sp, r1.sourcePartition()),
                () -> Assertions.assertArrayEquals(new byte[] {'a'}, (byte[]) r1.key()),
                () -> Assertions.assertArrayEquals(new byte[] {'b'}, (byte[]) r1.value()));
        Map<String, ?> so1 = r1.sourceOffset();
        Assertions.assertNotNull(so1);
        Assertions.assertAll(
                () -> Assertions.assertEquals(1L, so1.get(RollingFileReader.LINES_READ_OFFSETS)),
                () -> Assertions.assertEquals(l1.length() + (long) RollingFileWriter.RECORD_SEPARATOR.length, so1.get(RollingFileReader.CHARS_READ_OFFSETS)));
    }

    @Test
    void readIgnoreTimestamps() throws IOException {
        String l1 = l(100L, "a", "b");
        Map<String, ?> sp = initTest(0, 0, l1);
        SourceRecord r1 = test.nextRecord(true);
        Assertions.assertAll(
                () -> Assertions.assertNotNull(r1),
                () -> Assertions.assertNull(test.nextRecord(false)));
        Assertions.assertAll(
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.keySchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r1.valueSchema()),
                () -> Assertions.assertNull(r1.kafkaPartition()),
                () -> Assertions.assertNull(r1.timestamp()),
                () -> Assertions.assertEquals(sp, r1.sourcePartition()),
                () -> Assertions.assertArrayEquals(new byte[] {'a'}, (byte[]) r1.key()),
                () -> Assertions.assertArrayEquals(new byte[] {'b'}, (byte[]) r1.value()));
        Map<String, ?> so1 = r1.sourceOffset();
        Assertions.assertNotNull(so1);
        Assertions.assertAll(
                () -> Assertions.assertEquals(1L, so1.get(RollingFileReader.LINES_READ_OFFSETS)),
                () -> Assertions.assertEquals(l1.length() + (long) RollingFileWriter.RECORD_SEPARATOR.length, so1.get(RollingFileReader.CHARS_READ_OFFSETS)));
    }

    @Test
    void readWithInitialOffset() throws IOException {
        String l1 = l("a", "b");
        String l2 = l("c", "d");
        // lines are not important
        long linesRead = 10;
        long charsRead = l1.length() + RollingFileWriter.RECORD_SEPARATOR.length;
        Map<String, ?> sp = initTest(charsRead, linesRead, l1, l2);
        SourceRecord r2 = test.nextRecord(false);
        Assertions.assertAll(
                () -> Assertions.assertNotNull(r2),
                () -> Assertions.assertNull(test.nextRecord(false)));
        Assertions.assertAll(
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r2.keySchema()),
                () -> Assertions.assertSame(Schema.BYTES_SCHEMA, r2.valueSchema()),
                () -> Assertions.assertNull(r2.kafkaPartition()),
                () -> Assertions.assertNull(r2.timestamp()),
                () -> Assertions.assertEquals(sp, r2.sourcePartition()),
                () -> Assertions.assertArrayEquals(new byte[] {'c'}, (byte[]) r2.key()),
                () -> Assertions.assertArrayEquals(new byte[] {'d'}, (byte[]) r2.value()));
        Map<String, ?> so2 = r2.sourceOffset();
        Assertions.assertNotNull(so2);
        Assertions.assertAll(
                () -> Assertions.assertEquals(linesRead + 1L, so2.get(RollingFileReader.LINES_READ_OFFSETS)),
                () -> Assertions.assertEquals(charsRead + l2.length() + RollingFileWriter.RECORD_SEPARATOR.length, so2.get(RollingFileReader.CHARS_READ_OFFSETS)));
    }

    private String l(Long timestamp, String key, String value) {
        return (timestamp == null ? RollingFileWriter.NO_TIMESTAMP : timestamp)
                + RollingFileWriter.KEY_VALUE_SEPARATOR
                + Base64.getEncoder().encodeToString(key.getBytes())
                + RollingFileWriter.KEY_VALUE_SEPARATOR
                + Base64.getEncoder().encodeToString(value.getBytes());
    }

    private String l(String key, String value) {
        return l(null, key, value);
    }

    private Map<String, String> initTest(long charsRead, long linesRead, String... lines) throws IOException {
        File data = new File(base, "data_" + ++idx + ".txt");
        try (FileOutputStream fw = new FileOutputStream(data)) {
            for (String l : lines) {
                fw.write(l.getBytes());
                fw.write(RollingFileWriter.RECORD_SEPARATOR);
            }
            fw.flush();
        }
        Map<String, Object> map = new HashMap<>();
        map.put(RollingFileReader.LINES_READ_OFFSETS, linesRead);
        map.put(RollingFileReader.CHARS_READ_OFFSETS, charsRead);
        OffsetStorageReader osr = Mockito.mock(OffsetStorageReader.class);
        Mockito.doThrow(new AssertionError("wrong parameter")).when(osr).offset(Mockito.any());
        Mockito.doThrow(new AssertionError("wrong method")).when(osr).offsets(Mockito.any());
        Map<String, String> sourcePartition = Collections.singletonMap(RollingFileReader.FILE_NAME_KEY, data.getAbsolutePath());
        Mockito.doReturn(map).when(osr).offset(Mockito.eq(sourcePartition));
        test = new RollingFileReader(data, "topic", null, osr);
        return sourcePartition;
    }
}

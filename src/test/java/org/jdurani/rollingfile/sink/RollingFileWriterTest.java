package org.jdurani.rollingfile.sink;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jdurani.rollingfile.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RollingFileWriterTest {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriterTest.class);
    private final TopicPartition tp = new TopicPartition("t", 0);
    private String dir;

    @BeforeEach
    void setUp() throws IOException {
        dir = Utils.tmpDir(true).toFile().getAbsolutePath();
    }

    @Test
    void rollAfterOne() throws IOException {
        RollingFileWriter w = new RollingFileWriter(tp, dir, 1, Long.MAX_VALUE);
        SinkRecord r = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r);
        validateFinalFile(r);
    }

    @Test
    void rollAfterTwo() throws IOException {
        RollingFileWriter w = new RollingFileWriter(tp, dir, 2, Long.MAX_VALUE);
        SinkRecord r = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r);
        validateNotRoll(r);
        w.write(r);
        validateFinalFile(r);
    }

    @Test
    void rollAfter100ms() throws IOException, InterruptedException {
        long flushMs = 100;
        RollingFileWriter w = new RollingFileWriter(tp, dir, Long.MAX_VALUE, flushMs);
        SinkRecord r = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r);
        validateNotRoll(r);
        w.write(r);
        validateNotRoll(r);
        Thread.sleep(5 * flushMs); // there is background thread - let is little space to do its job
        LOG.info("Checking files.");
        validateFinalFile(r);
    }

    @Test
    void rollMultipleFiles() throws IOException {
        RollingFileWriter w = new RollingFileWriter(tp, dir, 2, Long.MAX_VALUE);
        SinkRecord r1 = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r1);
        validateNotRoll(r1);
        w.write(r1);
        validateFinalFile(r1);
        SinkRecord r2 = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 1);
        w.write(r2);
        validateFinalFile(r1);
        validateNotRoll(r2);
        w.write(r2);
        validateFinalFile(r2);
    }

    @Test
    void close() throws IOException {
        RollingFileWriter w = new RollingFileWriter(tp, dir, Long.MAX_VALUE, Long.MAX_VALUE);
        SinkRecord r = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r);
        validateNotRoll(r);
        w.close();
        validateFinalFile(r);
    }

    @Test
    void writeWrongData() {
        RollingFileWriter w = new RollingFileWriter(tp, dir, Long.MAX_VALUE, Long.MAX_VALUE);
        Assertions.assertAll(
                // wrong topic
                () -> Assertions.assertThrows(IllegalStateException.class, () -> w.write(new SinkRecord(tp.topic() + "x", tp.partition(), null, null, null, null, 0L))),
                // wrong partition
                () -> Assertions.assertThrows(IllegalStateException.class, () -> w.write(new SinkRecord(tp.topic(), tp.partition() + 1, null, null, null, null, 0L))),
                // wrong key class
                () -> Assertions.assertThrows(IllegalArgumentException.class, () -> w.write(new SinkRecord(tp.topic(), tp.partition(), null, new Object(), null, null, 0L))),
                // wrong value class
                () -> Assertions.assertThrows(IllegalArgumentException.class, () -> w.write(new SinkRecord(tp.topic(), tp.partition(), null, null, null, new Object(), 0L)))
        );
    }

    @Test
    void write() throws IOException {
        RollingFileWriter w = new RollingFileWriter(tp, dir, 2, Long.MAX_VALUE);
        byte[] key1 = {0};
        byte[] value1 = {0, 1, 2};
        byte[] key2 = {0, 1};
        byte[] value2 = {0, 1, 2, 3};

        SinkRecord r1 = new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, 0);
        w.write(r1);
        w.write(new SinkRecord(tp.topic(), tp.partition(), null, value1, null, key1, 1));
        validateFinalFile(r1);

        SinkRecord r2 = new SinkRecord(tp.topic(), tp.partition(), null, key1, null, value1, 2);
        w.write(r2);
        w.write(new SinkRecord(tp.topic(), tp.partition(), null, key2, null, value2, 3));
        validateFinalFile(r2);

        Assertions.assertEquals(
                Base64.getEncoder().encodeToString(new byte[0])
                        + RollingFileWriter.KEY_VALUE_SEPARATOR
                        + Base64.getEncoder().encodeToString(new byte[0])
                        + new String(RollingFileWriter.RECORD_SEPARATOR)
                        + Base64.getEncoder().encodeToString(value1)
                        + RollingFileWriter.KEY_VALUE_SEPARATOR
                        + Base64.getEncoder().encodeToString(key1)
                        + new String(RollingFileWriter.RECORD_SEPARATOR),
                readFile(r1));
        Assertions.assertEquals(
                Base64.getEncoder().encodeToString(key1)
                        + RollingFileWriter.KEY_VALUE_SEPARATOR
                        + Base64.getEncoder().encodeToString(value1)
                        + new String(RollingFileWriter.RECORD_SEPARATOR)
                        + Base64.getEncoder().encodeToString(key2)
                        + RollingFileWriter.KEY_VALUE_SEPARATOR
                        + Base64.getEncoder().encodeToString(value2)
                        + new String(RollingFileWriter.RECORD_SEPARATOR),
                readFile(r2));
    }

    private String readFile(SinkRecord r) throws IOException {
        try (FileInputStream fis = new FileInputStream(getExpectedFile(r));
                BufferedInputStream bis = new BufferedInputStream(fis);
                ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[256];
            int read;
            while ((read = bis.read(buf)) >= 0) {
                bos.write(buf, 0, read);
            }
            return bos.toString();
        }
    }

    private void validateFinalFile(SinkRecord r) {
        File expectedFile = getExpectedFile(r);
        File expectedTmpFile = new File(expectedFile.getParentFile(), ".tmp-" + expectedFile.getName() + ".tmp");
        Assertions.assertTrue(Paths.get(dir, tp.topic(), String.format(RollingFileWriter.FILE_NAME_FORMAT, tp.partition(), r.kafkaOffset())).toFile().getAbsoluteFile().exists(), "file " + expectedFile.getAbsolutePath() + " not exist");
        Assertions.assertFalse(expectedTmpFile.exists(), "tmp file " + expectedTmpFile.getAbsolutePath() + " still exists");
    }

    private void validateNotRoll(SinkRecord r) {
        File expectedFile = getExpectedFile(r);
        File expectedTmpFile = new File(expectedFile.getParentFile(), ".tmp-" + expectedFile.getName() + ".tmp");
        Assertions.assertFalse(Paths.get(dir, tp.topic(), String.format(RollingFileWriter.FILE_NAME_FORMAT, tp.partition(), r.kafkaOffset())).toFile().getAbsoluteFile().exists(), "file " + expectedFile.getAbsolutePath() + " exist");
        Assertions.assertTrue(expectedTmpFile.exists(), "tmp file " + expectedTmpFile.getAbsolutePath() + " missing");
    }

    private File getExpectedFile(SinkRecord r) {
        return Paths.get(dir, tp.topic(), String.format(RollingFileWriter.FILE_NAME_FORMAT, tp.partition(), r.kafkaOffset())).toFile().getAbsoluteFile();
    }
}

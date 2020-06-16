package org.jdurani.rollingfile.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.jdurani.rollingfile.RollingFileConfig;
import org.jdurani.rollingfile.Utils;
import org.jdurani.rollingfile.VersionHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RollingFileSourceTaskTest {

    private static final String SUFFIX = "-0000000000000000000.txt";
    private RollingFileSourceTask test;
    private String tmpDir;

    @BeforeEach
    void setUp() throws IOException {
        test = Mockito.spy(RollingFileSourceTask.class);
        SourceTaskContext c = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader osr = Mockito.mock(OffsetStorageReader.class);
        Mockito.doReturn(osr).when(c).offsetStorageReader();
        test.initialize(c);
        tmpDir = Utils.tmpDir(true).toFile().getAbsolutePath();
        Map<String, String> m = new HashMap<>();
        m.put(RollingFileConfig.ROLLING_FILE_DIRECTORY_CONFIG, tmpDir);
        m.put(RollingFileConfig.ROLLING_FILE_BATCH_SIZE_CONFIG, "2");
        test.start(m);
    }

    @Test
    void version() {
        Assertions.assertSame(VersionHolder.VERSION, test.version());
    }

    @Test
    void poll() throws IOException {
        List<RollingFileReader> rs = new ArrayList<>();
        RollingFileReader r1 = Mockito.mock(RollingFileReader.class);
        RollingFileReader r2 = Mockito.mock(RollingFileReader.class);
        rs.add(r1);
        rs.add(r2);
        Mockito.doReturn(rs).when(test).nextFilesToRead();

        SourceRecord r1s1 = Mockito.mock(SourceRecord.class);
        SourceRecord r1s2 = Mockito.mock(SourceRecord.class);
        SourceRecord r1s3 = Mockito.mock(SourceRecord.class);
        SourceRecord r2s1 = Mockito.mock(SourceRecord.class);
        SourceRecord r2s2 = Mockito.mock(SourceRecord.class);
        Mockito.doReturn(r1s1, r1s2, r1s3, null).when(r1).nextRecord();
        Mockito.doReturn(r2s1, r2s2, null).when(r2).nextRecord();

        List<SourceRecord> polledFirst = test.poll();
        Assertions.assertNotNull(polledFirst);
        Assertions.assertEquals(2, polledFirst.size());
        Assertions.assertAll(
                () -> Assertions.assertSame(r1s1, polledFirst.get(0)),
                () -> Assertions.assertSame(r1s2, polledFirst.get(1)));

        List<SourceRecord> polledSecond = test.poll();
        Assertions.assertNotNull(polledSecond);
        Assertions.assertEquals(2, polledSecond.size());
        Assertions.assertAll(
                () -> Assertions.assertSame(r1s3, polledSecond.get(0)),
                () -> Assertions.assertSame(r2s1, polledSecond.get(1)));

        List<SourceRecord> polledThird = test.poll();
        Assertions.assertNotNull(polledThird);
        Assertions.assertEquals(1, polledThird.size());
        Assertions.assertSame(r2s2, polledThird.get(0));

        Mockito.doReturn(Collections.emptyList()).when(test).nextFilesToRead();

        List<SourceRecord> polled = test.poll();
        Assertions.assertNotNull(polled);
        Assertions.assertEquals(0, polled.size());
    }

    @Test
    void nextFilesToRead() throws IOException {
        File t1 = new File(tmpDir, "topic1");
        File t2 = new File(tmpDir, "topic2");
        File t3 = new File(tmpDir, "topic3");

        t1.mkdirs();
        t2.mkdirs();
        t3.createNewFile();

        File t1f1 = new File(t1, "000000000" + "1" + SUFFIX);
        File t1f2 = new File(t1, "000000000" + "0" + SUFFIX);
        File t2f1 = new File(t1, "00000000" + "10" + SUFFIX);

        t1f1.createNewFile();
        t1f2.createNewFile();
        t2f1.createNewFile();

        List<RollingFileReader> out1 = test.nextFilesToRead();
        Assertions.assertNotNull(out1);
        Assertions.assertEquals(3, out1.size());
        Assertions.assertAll(
                () -> Assertions.assertNotNull(out1.get(0)),
                () -> Assertions.assertNotNull(out1.get(1)),
                () -> Assertions.assertNotNull(out1.get(2)));
        Assertions.assertAll( // order is important
                () -> Assertions.assertEquals(t1f2.getAbsolutePath(), out1.get(0).getFileAbsolutePath()),
                () -> Assertions.assertEquals(t1f1.getAbsolutePath(), out1.get(1).getFileAbsolutePath()),
                () -> Assertions.assertEquals(t2f1.getAbsolutePath(), out1.get(2).getFileAbsolutePath()));
        // mark files as fully read
        Assertions.assertEquals(0, test.poll().size());

        File t1f3 = new File(t1, "000000000" + "2" + SUFFIX);
        t1f3.createNewFile();

        List<RollingFileReader> out2 = test.nextFilesToRead();
        Assertions.assertNotNull(out2);
        Assertions.assertEquals(1, out2.size());
        Assertions.assertNotNull(out2.get(0));
        Assertions.assertEquals(t1f3.getAbsolutePath(), out2.get(0).getFileAbsolutePath());
        // mark files as fully read
        Assertions.assertEquals(0, test.poll().size());

        File t1f4 = new File(t1, "000000000" + "3" + SUFFIX);
        File t2f2 = new File(t1, "00000000" + "20" + SUFFIX);
        Assertions.assertTrue(t1f4.createNewFile(), "file not created");
        Assertions.assertTrue(t2f2.createNewFile(), "file not created");

        List<RollingFileReader> out3 = test.nextFilesToRead();
        Assertions.assertNotNull(out3);
        Assertions.assertEquals(2, out3.size());
        Assertions.assertAll(
                () -> Assertions.assertNotNull(out3.get(0)),
                () -> Assertions.assertNotNull(out3.get(1)));
        Assertions.assertAll(
                () -> Assertions.assertEquals(t1f4.getAbsolutePath(), out3.get(0).getFileAbsolutePath()),
                () -> Assertions.assertEquals(t2f2.getAbsolutePath(), out3.get(1).getFileAbsolutePath()));
        // mark files as fully read
        Assertions.assertEquals(0, test.poll().size());

        List<RollingFileReader> out4 = test.nextFilesToRead();
        Assertions.assertNotNull(out4);
        Assertions.assertEquals(0, out4.size());
    }
}

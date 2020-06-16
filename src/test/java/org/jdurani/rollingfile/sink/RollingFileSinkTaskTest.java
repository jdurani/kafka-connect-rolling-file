package org.jdurani.rollingfile.sink;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jdurani.rollingfile.VersionHolder;
import org.jdurani.rollingfile.exception.CloseException;
import org.jdurani.rollingfile.exception.FlushException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class RollingFileSinkTaskTest {

    private RollingFileSinkTask test;

    @BeforeEach
    void setUp() {
        test = Mockito.spy(RollingFileSinkTask.class);
    }

    @Test
    void version() {
        Assertions.assertSame(VersionHolder.VERSION, test.version());
    }

    @Test
    void putAndFlush() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2).when(test).getWriter(Mockito.any());
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verifyNoMoreInteractions(w1, w2);

        test.flush(null);
        Mockito.verify(w1).flush();
        Mockito.verify(w2).flush();
        Mockito.verifyNoMoreInteractions(w1, w2);
    }

    @Test
    void putAndFlushThrow() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w3 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2, w3).when(test).getWriter(Mockito.any());
        Mockito.doThrow(new IOException("Expected")).when(w2).flush();
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        SinkRecord s3 = new SinkRecord("", 2, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2, s3));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verify(w3).write(Mockito.same(s3));
        Mockito.verifyNoMoreInteractions(w1, w2, w3);

        Assertions.assertThrows(FlushException.class, () -> test.flush(null));
        Mockito.verify(w1).flush();
        Mockito.verify(w2).flush();
        Mockito.verify(w3).flush();
        Mockito.verifyNoMoreInteractions(w1, w2, w3);
    }

    @Test
    void putAndClose() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2).when(test).getWriter(Mockito.any());
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verifyNoMoreInteractions(w1, w2);

        test.close(null);
        flushAndCloseInOrder(w1);
        flushAndCloseInOrder(w2);
    }

    @Test
    void putAndClosePartial() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w3 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w4 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2, w3, w4).when(test).getWriter(Mockito.any());
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        SinkRecord s3 = new SinkRecord("", 2, null, null, null, null, 0);
        SinkRecord s4 = new SinkRecord("", 3, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2, s3, s4));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verify(w3).write(Mockito.same(s3));
        Mockito.verify(w4).write(Mockito.same(s4));
        Mockito.verifyNoMoreInteractions(w1, w2, w3, w4);

        test.close(Arrays.asList(new TopicPartition("", 0), new TopicPartition("", 2)));
        flushAndCloseInOrder(w1);
        flushAndCloseInOrder(w3);
        Mockito.verifyNoMoreInteractions(w2, w4);
    }

    @Test
    void putAndCloseFlushThrow() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w3 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2, w3).when(test).getWriter(Mockito.any());
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        SinkRecord s3 = new SinkRecord("", 3, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2, s3));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verify(w3).write(Mockito.same(s3));
        Mockito.verifyNoMoreInteractions(w1, w2, w3);

        Mockito.doThrow(new IOException("Expected")).when(w2).flush();
        Assertions.assertThrows(CloseException.class, () -> test.close(null));
        flushAndCloseInOrder(w1);
        flushAndCloseInOrder(w3);
        Mockito.verify(w2).flush();
        Mockito.verifyNoMoreInteractions(w2);
    }

    @Test
    void putAndCloseCloseThrow() throws IOException {
        RollingFileWriter w1 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w2 = Mockito.mock(RollingFileWriter.class);
        RollingFileWriter w3 = Mockito.mock(RollingFileWriter.class);
        Mockito.doReturn(w1, w2, w3).when(test).getWriter(Mockito.any());
        SinkRecord s1 = new SinkRecord("", 0, null, null, null, null, 0);
        SinkRecord s2 = new SinkRecord("", 1, null, null, null, null, 0);
        SinkRecord s3 = new SinkRecord("", 3, null, null, null, null, 0);
        test.put(Arrays.asList(s1, s2, s3));
        Mockito.verify(w1).write(Mockito.same(s1));
        Mockito.verify(w2).write(Mockito.same(s2));
        Mockito.verify(w3).write(Mockito.same(s3));
        Mockito.verifyNoMoreInteractions(w1, w2, w3);

        Mockito.doThrow(new IOException("Expected")).when(w2).close();
        Assertions.assertThrows(CloseException.class, () -> test.close(null));
        flushAndCloseInOrder(w1);
        flushAndCloseInOrder(w2);
        flushAndCloseInOrder(w3);
    }

    private void flushAndCloseInOrder(RollingFileWriter w) throws IOException {
        InOrder io = Mockito.inOrder(w);
        io.verify(w).flush();
        io.verify(w).close();
        io.verifyNoMoreInteractions();
    }
}

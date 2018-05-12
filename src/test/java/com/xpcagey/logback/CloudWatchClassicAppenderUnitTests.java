package com.xpcagey.logback;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.StatusManager;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * These tests verify the behavior of the CloudWatchAppender when it is configured properly.
 */
public class CloudWatchClassicAppenderUnitTests {
    @Test
    public void initialValuesTest() {
        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        assertNotNull(appender.getStreamNameSupplier());
        assertNotNull(appender.getAWSLogsSupplier());
        assertEquals("", appender.getGroupName());
        assertEquals("", appender.getStreamName());
        assertNull(appender.getEncoder());
    }

    @Test
    public void lifecycleTest() {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenAnswer(inv -> {
            DescribeLogStreamsRequest req = inv.getArgument(0);
            return new DescribeLogStreamsResult().withLogStreams(
                    new LogStream()
                            .withLogStreamName(req.getLogStreamNamePrefix() + "-suffix")
                            .withUploadSequenceToken("second"),
                    new LogStream()
                            .withLogStreamName(req.getLogStreamNamePrefix())
                            .withUploadSequenceToken("first"));
        });

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);

        Context context = new LoggerContext();

        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        appender.setContext(context);
        appender.setAWSLogsSupplier(builder);
        appender.setGroupName("testGroup");
        appender.setStreamName("testStream");
        appender.setEncoder(encoder);
        assertFalse(appender.isStarted()); // we are in our initial state
        appender.start();
        assertTrue(appender.isStarted()); // we have established a connection
        appender.start(); // we should be allowed to re-enter without penalty
        assertTrue(appender.isStarted()); // we have still established a connection
        appender.stop();
        appender.stop();
        assertFalse(appender.isStarted()); // we have shut down our connection
        appender.stop(); // we should be allowed to re-exit without penalty
        assertFalse(appender.isStarted()); // we have still shut down our connection

        verify(encoder).headerBytes();
        verify(encoder).footerBytes();
        verifyNoMoreInteractions(encoder);

        verify(builder).get();
        verifyNoMoreInteractions(builder);

        verify(logs).describeLogStreams(new DescribeLogStreamsRequest()
                .withLogGroupName(appender.getGroupName())
                .withLogStreamNamePrefix(appender.getStreamName()));
        verifyNoMoreInteractions(logs);
    }

    @Test
    public void steamCreationTest() {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenReturn(new DescribeLogStreamsResult());
        when(logs.createLogStream(any())).thenReturn(new CreateLogStreamResult());

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);

        LoggerContext context = new LoggerContext();

        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        appender.setContext(context);
        appender.setAWSLogsSupplier(builder);
        appender.setGroupName("testGroup");
        appender.setStreamName("testStream");
        appender.setEncoder(encoder);

        appender.start();
        assertTrue(appender.isStarted()); // we have established a connection
        appender.stop();
        assertFalse(appender.isStarted()); // we have shut down our connection

        verify(encoder).headerBytes();
        verify(encoder).footerBytes();
        verifyNoMoreInteractions(encoder);

        verify(builder).get();
        verifyNoMoreInteractions(builder);

        verify(logs).describeLogStreams(new DescribeLogStreamsRequest()
                .withLogGroupName(appender.getGroupName())
                .withLogStreamNamePrefix(appender.getStreamName()));
        verify(logs).createLogStream(new CreateLogStreamRequest()
                .withLogGroupName(appender.getGroupName())
                .withLogStreamName(appender.getStreamName()));
        verifyNoMoreInteractions(logs);
    }

    @Test
    public void appendTest() {
        List<String> read = new ArrayList<>();
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenAnswer(inv -> {
            DescribeLogStreamsRequest req = inv.getArgument(0);
            return new DescribeLogStreamsResult().withLogStreams(new LogStream()
                    .withLogStreamName(req.getLogStreamNamePrefix())
                    .withUploadSequenceToken("first"));
        });
        when(logs.putLogEvents(any())).thenAnswer(inv -> {
            PutLogEventsRequest req = inv.getArgument(0);
            for (final InputLogEvent ev : req.getLogEvents())
                read.add(ev.getMessage());
            return new PutLogEventsResult().withNextSequenceToken(Integer.toString(read.size()));
        });

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getFormattedMessage()).thenReturn("event1", "event2", "event3");
        when(event.getTimeStamp()).thenReturn(System.currentTimeMillis());

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn("header".getBytes());
        when(encoder.footerBytes()).thenReturn("footer".getBytes());
        when(encoder.encode(any())).thenAnswer(inv -> ((ILoggingEvent)inv.getArgument(0)).getFormattedMessage().getBytes());

        Supplier<String> source = mock(StringSupplier.class);
        when(source.get()).thenReturn("testStream");

        LoggerContext context = new LoggerContext();

        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        appender.setContext(context);
        appender.setAWSLogsSupplier(builder);
        appender.setGroupName("testGroup");
        appender.setStreamNameSupplier(source);
        appender.setEncoder(encoder);

        appender.doAppend(event); // should warn and reject (not started)

        appender.start();
        assertTrue(appender.isStarted()); // we have established a connection
        appender.doAppend(event);
        appender.doAppend(event);
        appender.doAppend(event);
        appender.stop();
        assertFalse(appender.isStarted()); // we have shut down our connection

        appender.doAppend(event); // should warn and reject (closed)

        verify(source).get(); // ask once at startup, not per call
        verifyNoMoreInteractions(source);

        verify(encoder).headerBytes();
        verify(encoder, times(3)).encode(any());
        verify(encoder).footerBytes();
        verifyNoMoreInteractions(encoder);

        verify(builder).get();
        verifyNoMoreInteractions(builder);

        verify(logs).describeLogStreams(new DescribeLogStreamsRequest()
                .withLogGroupName(appender.getGroupName())
                .withLogStreamNamePrefix(appender.getStreamName()));
        verify(logs, atLeast(2)).putLogEvents(any());
        verify(logs, atMost(5)).putLogEvents(any());
        verifyNoMoreInteractions(logs);

        assertArrayEquals(new Object[]{"header", "event1", "event2", "event3", "footer"}, read.toArray());
    }

    @Test
    public void stopExecutionExceptionTest() throws InterruptedException, ExecutionException {
        StatusManager manager = mock(StatusManager.class);
        Context context = mock(Context.class);
        when(context.getStatusManager()).thenReturn(manager);

        CloudWatchWorker<ILoggingEvent> worker = mock(ILoggingEventWorker.class);
        doThrow(ExecutionException.class).when(worker).join();

        // eat exception, report as error
        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        appender.setContext(context);
        appender.startWith(worker, appender::onStop);
        appender.stop();

        verify(worker).start(any());
        verify(worker).requestStop();
        verify(worker).join();
        verifyNoMoreInteractions(worker);

        verify(context).getStatusManager();
        verifyNoMoreInteractions(context);

        verify(manager).add(any(ErrorStatus.class));
        verifyNoMoreInteractions(manager);
    }

    @Test
    public void stopInterruptedException() throws InterruptedException, ExecutionException {
        StatusManager manager = mock(StatusManager.class);
        Context context = mock(Context.class);
        when(context.getStatusManager()).thenReturn(manager);

        CloudWatchWorker<ILoggingEvent> worker = mock(ILoggingEventWorker.class);
        doThrow(InterruptedException.class).when(worker).join();

        // eat exception, report as error
        CloudWatchClassicAppender appender = new CloudWatchClassicAppender();
        appender.setContext(context);
        appender.startWith(worker, appender::onStop);
        appender.stop();

        verify(worker).start(any());
        verify(worker).requestStop();
        verify(worker).join();
        verifyNoMoreInteractions(worker);

        verify(context).getStatusManager();
        verifyNoMoreInteractions(context);

        verify(manager).add(any(ErrorStatus.class));
        verifyNoMoreInteractions(manager);

        assertTrue(Thread.interrupted());
    }
}

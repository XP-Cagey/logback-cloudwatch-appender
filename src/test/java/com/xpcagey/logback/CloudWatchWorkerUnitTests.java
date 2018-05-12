package com.xpcagey.logback;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.ContextAware;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CloudWatchWorkerUnitTests {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * We always have a valid builder and context, and we know that groupName and streamName are not null. We need to
     * check the validity of groupName, streamName, and encoder.
     */
    @Test
    public void testGroupNameLegal() {
        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        ContextAware aware = mock(ContextAware.class);

        thrown.expect(IllegalArgumentException.class);
        new CloudWatchWorker<>(builder, "", "streamName", encoder, null, aware);
    }

    @Test
    public void testStreamNameLegal() {
        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        ContextAware aware = mock(ContextAware.class);

        thrown.expect(IllegalArgumentException.class);
        new CloudWatchWorker<>(builder, "groupName", "", encoder, null, aware);
    }

    @Test
    public void testEncoderLegal() {
        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        ContextAware aware = mock(ContextAware.class);

        thrown.expect(IllegalArgumentException.class);
        new CloudWatchWorker<ILoggingEvent>(builder, "groupName", "streamName", null, null, aware);
    }

    @Test
    public void testJoinUnstarted() throws InterruptedException, ExecutionException{
        AWSLogs logs = mock(AWSLogs.class);
        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);

        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        worker.join(); // should not block, should have no negative side effects
    }

    @Test
    public void testInvalidParameters() {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenThrow(new InvalidParameterException("invalid"));

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        ContextAware aware = mock(ContextAware.class);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Bad argument passed to Amazon CloudWatch");
        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        worker.start(this::doNothing);
    }

    @Test
    public void testResourceNotFound() {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenThrow(new ResourceNotFoundException("invalid"));

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        ContextAware aware = mock(ContextAware.class);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Log Group [groupName] not found");
        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        worker.start(this::doNothing);
    }

    @Test
    public void testResourceAlreadyExists() {
        AWSLogs logs = mock(AWSLogs.class);
        // simulate a creation collision
        when(logs.describeLogStreams(any()))
                .thenReturn(new DescribeLogStreamsResult().withLogStreams())
                .thenAnswer(inv -> {
                    DescribeLogStreamsRequest req = inv.getArgument(0);
                    return new DescribeLogStreamsResult().withLogStreams(new LogStream()
                            .withLogStreamName(req.getLogStreamNamePrefix())
                            .withUploadSequenceToken("first"));
                });
        when(logs.createLogStream(any())).thenThrow(new ResourceAlreadyExistsException("invalid"));

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        try {
            worker.start(this::doNothing);
        } finally {
            stopAndHandle(worker);
        }
        verify(logs, times(2)).describeLogStreams(any());
        verify(logs).createLogStream(any());
        verifyNoMoreInteractions(logs);
    }

    @Test
    public void testStartedTwice() {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenReturn(new DescribeLogStreamsResult().withLogStreams());
        when(logs.createLogStream(any())).thenReturn(new CreateLogStreamResult());
        when(logs.putLogEvents(any())).thenReturn(new PutLogEventsResult().withNextSequenceToken("123"));

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        thrown.expect(IllegalStateException.class);
        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        try {
            worker.start(this::doNothing);
            worker.start(this::doNothing); //throws
        } finally {
            stopAndHandle(worker);
        }
    }

    @Test
    public void testDataAlreadyAccepted() {
        DataAlreadyAcceptedException ex = mock(DataAlreadyAcceptedException.class);
        when(ex.getExpectedSequenceToken()).thenReturn("123");

        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenReturn(new DescribeLogStreamsResult().withLogStreams());
        when(logs.createLogStream(any())).thenReturn(new CreateLogStreamResult());
        when(logs.putLogEvents(any())).thenThrow(ex);

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);
        when(encoder.encode(any())).thenReturn(new byte[0]);

        ILoggingEvent event = mock(ILoggingEvent.class);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        try {
            worker.start(this::doNothing);
            worker.add(event);
        } finally {
            stopAndHandle(worker);
        }

        verify(ex).getExpectedSequenceToken();
        verifyNoMoreInteractions(ex);
    }

    @Test
    public void testInvalidSequenceToken() throws ExecutionException, InterruptedException {
        InvalidSequenceTokenException ex = mock(InvalidSequenceTokenException.class);
        when(ex.getExpectedSequenceToken()).thenReturn("123");

        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenReturn(new DescribeLogStreamsResult().withLogStreams());
        when(logs.createLogStream(any())).thenReturn(new CreateLogStreamResult());
        when(logs.putLogEvents(any())).thenThrow(ex);

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);
        when(encoder.encode(any())).thenReturn(new byte[0]);

        ILoggingEvent event = mock(ILoggingEvent.class);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        thrown.expect(ExecutionException.class);
        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        worker.start(this::doNothing);
        worker.add(event);
        worker.requestStop();
        worker.join();

        verify(ex, times(CloudWatchWorker.MAX_RETRIES)).getExpectedSequenceToken();
        verifyNoMoreInteractions(ex);
    }

    @Test
    public void testServiceUnavailable() throws ExecutionException, InterruptedException {
        AWSLogs logs = mock(AWSLogs.class);
        when(logs.describeLogStreams(any())).thenReturn(new DescribeLogStreamsResult().withLogStreams());
        when(logs.createLogStream(any())).thenReturn(new CreateLogStreamResult());
        when(logs.putLogEvents(any())).thenThrow(new ServiceUnavailableException("invalid"));

        Supplier<AWSLogs> builder = mock(AWSLogsSupplier.class);
        when(builder.get()).thenReturn(logs);

        Encoder<ILoggingEvent> encoder = mock(ILoggingEventEncoder.class);
        when(encoder.headerBytes()).thenReturn(new byte[0]);
        when(encoder.footerBytes()).thenReturn(new byte[0]);
        when(encoder.encode(any())).thenReturn(new byte[0]);

        ILoggingEvent event = mock(ILoggingEvent.class);

        Context context = new LoggerContext();
        ContextAware aware = mock(ContextAware.class);
        when(aware.getContext()).thenReturn(context);

        thrown.expect(ExecutionException.class);
        CloudWatchWorker<ILoggingEvent> worker = new CloudWatchWorker<>(builder, "groupName", "streamName", encoder, this::getTimestamp, aware);
        worker.start(this::doNothing);
        worker.add(event);
        worker.requestStop();
        worker.join();
    }

    private void stopAndHandle(CloudWatchWorker worker) {
        worker.requestStop();
        try {
            worker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected thread interruption from join()", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected exception from join()", e);
        }
    }

    private long getTimestamp(ILoggingEvent e) {
        return e.getTimeStamp();
    }

    private void doNothing() {}
}

package com.xpcagey.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.ResourceNotFoundException;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * CloudWatchClassicAppender implements the logback <code>Appender</code> interface with a connection to Amazon
 * CloudWatch for Logback classic implementations. This class is a lightweight wrapper around a
 * <code>CloudWatchWorker</code> that pushes appended <code>ILoggingEvent</code>s into a <code>BlockingQueue</code> for
 * asynchronous processing.
 *
 * If you will be using this appender in a cluster, it is *strongly recommended* that you use the
 * <code>setStreamNameSupplier</code> interface to give each process a unique log stream name inside of your log group.
 * This code attempts to correct for stream write collisions, but the Amazon CloudWatch system expects that each log
 * producer has a distinct stream and this appender will need to retry every send that is written after another process
 * has touched a stream.
 * @see ILoggingEvent
 * @see Encoder
 */
public class CloudWatchClassicAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private static final AWSLogsClientBuilder builder = AWSLogsClient.builder();
    private static final Supplier<AWSLogs> fallbackSupplier = builder::build;
    private static final Supplier<String> fallbackSource = () -> "";
    private Encoder<ILoggingEvent> encoder;
    private Supplier<AWSLogs> supplier;
    private Supplier<String> source;
    private String groupName;
    volatile private CloudWatchWorker<ILoggingEvent> worker;

    /**
     * Get the encoder that was set via <code>setEncoder</code>
     * @return the supplied encoder
     */
    Encoder<ILoggingEvent> getEncoder() {
        return this.encoder;
    }

    /**
     * Get the group name supplied by <code>setGroupName</code>
     * @return the supplied group name
     */
    String getGroupName() {
        return (this.groupName == null) ? "" : this.groupName;
    }

    /**
     * Get the stream name that was either explicitly supplied by <code>setStreamName</code> or pulled from the supplier
     * passed to <code>setStreamNameSupplier</code>. If both functions have been called, the last one wins. If neither
     * function has been called, this returns the empty string.
     * @return the supplied stream name
     */
    String getStreamName() {
        return this.getStreamNameSupplier().get();
    }

    /**
     * Get the stream name supplier that was passed to <code>setStreamNameSupplier</code> or a static supplier wrapping
     * the currently set stream name.
     * @return a stream name supplier
     */
    Supplier<String> getStreamNameSupplier() {
        return (this.source == null) ? fallbackSource : this.source;
    }

    /**
     * Get the <code>AWSLogs</code>an alternative source for an <code>AWSLogs</code> to be used by this Appender. Allows the use of custom
     * credentials.
     * @return the set of
     */
    Supplier<AWSLogs> getAWSLogsSupplier() { return (this.supplier == null) ? fallbackSupplier : this.supplier; }

    /**
     * Sets the encoder to be used by this logging system; any encoder that is legal for an OutputStreamAppender may
     * also be used here.  The header and footer for the appender will be sent at startup and shutdown of this appender
     * if they are not empty.
     * @param encoder the encoder to use for logging
     */
    public void setEncoder(Encoder<ILoggingEvent> encoder) {
        this.encoder = encoder;
    }

    /**
     * Sets a static group name; if this group is not present an exception will be thrown during start().
     * @param groupName group name passed through the AWSLogs API
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Sets a static stream name; if this group is not present in the AWS account it will be manually created.
     * Should only be used when no stream name source has been set.
     * @param streamName stream name passed through the AWSLogs API
     */
    public void setStreamName(String streamName) {
        this.source = () -> streamName;
    }

    /**
     * Allows injection of a custom stream name creator that can use fields like IP address or EC2 instance id to
     * provide a unique stream name for each member of a cluster. If the generated name is not present in the AWS
     * account it will be manually created. Should only be used when no stream name has been explicitly set.
     * @param source generator for the stream name to use for this instance
     */
    public void setStreamNameSupplier(Supplier<String> source) {
        this.source = source;
    }

    /**
     * Allows injection of an <code>AWSLogs</code> that is built using custom credentials.
     * @param supplier the overloaded supplier that will be used to construct the worker at startup
     * @see AWSLogs
     */
    public void setAWSLogsSupplier(Supplier<AWSLogs> supplier) {
        this.supplier = supplier;
    }

    /**
     * Called by Logback during startup or reset; attempts to connect to AWSLogs and either create or use a log stream.
     * @throws IllegalArgumentException if the group name or stream name is rejected by Amazon CloudWatch or the encoder
     * has not been set
     * @throws ResourceNotFoundException if the log group has not been created in Amazon CloudWatch
     */
    @Override
    public void start() {
        if (isStarted())
            return;
        this.startWith(new CloudWatchWorker<>(getAWSLogsSupplier(), getGroupName(), getStreamName(), getEncoder(), this::getTimetstamp, this), this::onStop);
        addInfo("Initialization successful");
    }

    /**
     * Called by Logback during reset or shutdown; safely tears down the connection to AWSLogs
     */
    @Override
    public void stop() {
        if (!isStarted())
            return;
        try {
            worker.requestStop();
            worker.join();
            worker = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            addError("Thread interrupted while waiting for shutdown of CloudWatchAppender");
        } catch (ExecutionException e) {
            addError("CloudWatchAppender had a fatal error", e);
        }
    }

    /**
     * Calling this function will ship the event to AWSLogs
     */
    @Override
    protected void append(ILoggingEvent event) {
        event.prepareForDeferredProcessing();
        worker.add(event);
    }

    void startWith(CloudWatchWorker<ILoggingEvent> worker, Runnable stopHook) { // for injection by tests
        this.worker = worker;
        worker.start(stopHook);
        super.start();
    }

    void onStop() {
        super.stop();
    }

    private long getTimetstamp(ILoggingEvent e) {
        return e.getTimeStamp();
    }
}


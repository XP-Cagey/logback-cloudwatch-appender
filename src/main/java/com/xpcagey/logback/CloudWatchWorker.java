package com.xpcagey.logback;

import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.ContextAware;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * CloudWatchWorker encapsulates the background thread that is used to push logging events to Amazon CloudWatch. It
 * blocks until a log message is received and then attempts to bundle additional messages until the queue is emptied
 * (up to 100 messages per batch). Processing is terminated by passing a sentinel into the queue via the
 * <code>requestStop()</code> function.
 */
class CloudWatchWorker<T> {
    static final int MAX_RETRIES = 10;
    static final int MAX_BATCH_SIZE = 100;

    private final BlockingQueue<Optional<T>> queue = new LinkedBlockingQueue<>();
    private final AWSLogs logs;
    private final String groupName;
    private final String streamName;
    private final Encoder<T> encoder;
    private final ToLongFunction<T> timer;
    private final ContextAware parent;
    private IntPredicate retryHook = CloudWatchWorker::defaultRetryHook;
    private String sequenceToken;
    private Future<Void> future;

    /**
     * Constructor
     * @param builder supplier for the AWSLogs that will be used for sending
     * @param groupName the log group name to use
     * @param streamName the stream group name to use
     * @param encoder the encoder to use for message content
     * @param parent the context aware wrapper that can be used for starting the background thread and reporting errors
     * @throws IllegalArgumentException if group name or stream name are empty or the encoder or timer are null
     */
    CloudWatchWorker(Supplier<AWSLogs> builder, String groupName, String streamName, Encoder<T> encoder, ToLongFunction<T> timer, ContextAware parent) {
        if (groupName.isEmpty())
            throw new IllegalArgumentException("'groupName' must be set");
        if (streamName.isEmpty())
            throw new IllegalArgumentException("'streamName' or 'streamNameResolver' must be set");
        if (encoder == null)
            throw new IllegalArgumentException("'encoder' must be set");
        if (timer == null)
            throw new IllegalArgumentException("'timer' must be set");
        this.logs = builder.get();
        this.groupName = groupName;
        this.streamName = streamName;
        this.encoder = encoder;
        this.timer = timer;
        this.parent = parent;
    }

    /**
     * Attempt to begin processing; an internal <code>CompletableFuture</code> tracks the status of the system; the
     * <code>Runnable</code> passed to this function is automatically called when processing is finished.
     * @param stopHook called automatically when processing has stopped; allows the surrounding appender is
     */
    synchronized void start(Runnable stopHook) {
        if (this.future != null)
            throw new IllegalStateException("Attempt to start the same CloudWatchAppender.Worker twice");

        setSequenceToken();
        byte[] header = encoder.headerBytes();
        if (header.length > 0)
            send(Collections.singleton(construct(System.currentTimeMillis(), header)));

        this.future = CompletableFuture.runAsync(this::run, parent.getContext().getScheduledExecutorService()).thenRun(stopHook);
    }

    /**
     * Call to add an event for publishing
     */
    void add(T event) { this.queue.add(Optional.of(event)); }

    /**
     * Call to begin the shutdown process, then call <code>join()</code> to wait for it to complete
     */
    void requestStop() { this.queue.add(Optional.empty()); }

    /**
     * Call to wait for processing to complete after calling <code>requestStop()</code>
     * @throws InterruptedException if the join was interrupted
     * @throws ExecutionException if the future returned an exception
     */
    void join() throws InterruptedException, ExecutionException {
        final Future<Void> future = this.future;
        if (future != null)
            future.get();
    }

    /**
     * This is the main processing loop for the background thread; it should not be referenced directly; see
     * <code>start</code> instead.
     */
    private void run() {
        List<InputLogEvent> list = new ArrayList<>();
        Optional<T> event;
        do {
            try {
                event = this.queue.take();
            } catch (InterruptedException e) {
                parent.addWarn("stopping due to thread interrupt");
                Thread.currentThread().interrupt();
                break;
            }
            list.clear();
            while (event != null) {
                if (event.isPresent()) {
                    list.add(construct(timer.applyAsLong(event.get()), encoder.encode(event.get())));
                    if (list.size() >= MAX_BATCH_SIZE)
                        break;
                    event = this.queue.poll();
                } else {
                    byte[] footer = encoder.footerBytes();
                    if (footer.length > 0)
                        list.add(construct(System.currentTimeMillis(), footer));
                    break;
                }
            }
            if (!list.isEmpty()) {
                list.sort(Comparator.comparingLong(InputLogEvent::getTimestamp));
                send(list);
            }
        } while (event == null || event.isPresent());
    }

    /**
     * Responsible for translating a time and message sequence into an <code>InputLogEvent</code> that the
     * <code>AWSLog</code> API can consume.
     * @param timestamp log message creation time to report to CloudWatch
     * @param encoded raw bytes for the message to report to CloudWatch
     * @return the log event to be used in <code>send()</code>
     */
    private InputLogEvent construct(long timestamp, byte[] encoded) {
        return new InputLogEvent().withTimestamp(timestamp).withMessage(new String(encoded, StandardCharsets.UTF_8));
    }

    /**
     * Implementation of the actual message send to AWSLogs; called from the <code>run()</code> loop as new log messages
     * are passed to the system.  Uses an optimized tail-recursive retry if AWS reports a collision in the sequence
     * token or service not available.  Uses a backoff retry of 200ms 204.8s for the service unavailable case.
     * @param events the ordered list of events to be added to the log
     * @throws RuntimeException if the service was not available or retries due to sequence problems were exceeded
     */
    private void send(Collection<InputLogEvent> events) {
        PutLogEventsRequest request = new PutLogEventsRequest()
                .withLogEvents(events)
                .withLogGroupName(this.groupName)
                .withLogStreamName(this.streamName);

        RuntimeException ex = null;
        for (int failures = 0; this.retryHook.test(failures); ++failures) {
            ex = null;
            if (this.sequenceToken != null)
                request.setSequenceToken(this.sequenceToken);

            try {
                PutLogEventsResult result = logs.putLogEvents(request);
                this.sequenceToken = result.getNextSequenceToken();
                break;
            } catch (DataAlreadyAcceptedException e) {
                this.sequenceToken = e.getExpectedSequenceToken();
                break;
            } catch (InvalidSequenceTokenException e) {
                this.sequenceToken = e.getExpectedSequenceToken();
                ex = e;
            } catch (ServiceUnavailableException e) {
                ex = e;
            }
        }

        // if we exited the loop with an exception active, throw it.
        if (ex != null)
            throw new RuntimeException("Retries exceeded", ex);
    }

    /**
     * Initial AWSLogs setup when <code>run()</code> is called; attempts to get a sequence token for the log stream and
     * then falls back to stream creation if necessary.
     * @throws IllegalArgumentException if a bad group name or stream name is rejected by Amazon CloudWatch
     * @throws ResourceNotFoundException if the log group has not been created in Amazon CloudWatch
     */
    private void setSequenceToken() {
        while (true) {
            try {
                parent.addInfo("Attempting to retrieve sequence number for group [" + this.groupName + "], stream [" + this.streamName + "]");
                DescribeLogStreamsResult result = logs.describeLogStreams(new DescribeLogStreamsRequest()
                        .withLogGroupName(this.groupName)
                        .withLogStreamNamePrefix(this.streamName));

                for (LogStream stream : result.getLogStreams()) {
                    if (this.streamName.equals(stream.getLogStreamName())) {
                        this.sequenceToken = stream.getUploadSequenceToken();
                        break;
                    }
                }

                // create a new stream if an existing one was not present
                if (this.sequenceToken == null) {
                    logs.createLogStream(new CreateLogStreamRequest()
                            .withLogStreamName(this.streamName)
                            .withLogGroupName(this.groupName));
                    this.sequenceToken = null;
                    parent.addInfo("Created stream [" +this.streamName + "]");
                }
                break;
            } catch (InvalidParameterException e) {
                throw new IllegalArgumentException("Bad argument passed to Amazon CloudWatch", e);
            } catch (ResourceNotFoundException e) {
                throw new IllegalArgumentException("Log Group [" + this.groupName + "] not found");
            } catch (ResourceAlreadyExistsException e) {
                // continue
            }
        }
    }

    /**
     * For testing - replaces the retry hook
     * @param hook the function to call when it is time to evaluate a retry
     */
    void setRetryHook(IntPredicate hook) { this.retryHook = hook; }

    /**
     * Returns whether a retry should be permitted and backs off the timer if it should
     * @param failures the number of attempts that have already failed
     * @return true if the send should be retried again
     */
    static boolean defaultRetryHook(int failures) {
        if (failures >= MAX_RETRIES)
            return false;
        if (failures == 0)
            return true;
        try {
            Thread.sleep(100 * 2 ^ failures);
            return true;
        } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}

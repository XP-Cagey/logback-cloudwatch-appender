package com.xpcagey.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.ContextAware;
import com.amazonaws.services.logs.AWSLogs;

import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Included to allow mocking without unchecked warnings.
 */
interface ILoggingEventEncoder extends Encoder<ILoggingEvent> {}
interface AWSLogsSupplier extends Supplier<AWSLogs> {}
interface StringSupplier extends Supplier<String> {}

class ILoggingEventWorker extends CloudWatchWorker<ILoggingEvent> {
    ILoggingEventWorker(Supplier<AWSLogs> builder, String groupName, String streamName, Encoder<ILoggingEvent> encoder, ToLongFunction<ILoggingEvent> timer, ContextAware parent) {
        super(builder, groupName, streamName, encoder, timer, parent);
    }
}

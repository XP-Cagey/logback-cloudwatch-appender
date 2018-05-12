package com.xpcagey.logback;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These tests require manually setting up an environment before they can succeed.  They assume that the default
 * credential provider can give this process IAM access to a CloudWatch log group named
 * "com.xpcagey.logback.CloudWatchAppenderIntegrationTests"; Once the log group has been manually created these tests
 * will automatically write to a stream named "Testing" inside of tha group so that you can verify that they are working
 * as intended. The destination of the integration tests can be customized by modifying the "logback-test.xml" file in
 * the resources section of this repository.
 */
@Category(IntegrationTest.class)
public class CloudWatchAppenderIntegrationTests {
    @Test
    public void parserTest() {
        Logger logger = LoggerFactory.getLogger(CloudWatchAppenderIntegrationTests.class);
        logger.info("Entering integration test.");
        logger.debug("Debug level information sent.");
        Logger panicLogger = LoggerFactory.getLogger("PanicLogger");
        panicLogger.error("The sky is falling!");
        logger.info("Leaving integration test.");
    }
}

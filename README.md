# Logback Appender for Amazon CloudWatch

[![Build Status](https://travis-ci.org/xp-cagey/logback-cloudwatch-appender.svg?branch=master)](https://travis-ci.org/xp-cagey/logback-cloudwatch-appender) [![codecov](https://codecov.io/gh/xp-cagey/logback-cloudwatch-appender/branch/master/graph/badge.svg)](https://codecov.io/gh/xp-cagey/logback-cloudwatch-appender) [![License: MIT](https://img.shields.io/github/license/xp-cagey/logback-cloudwatch-appender.svg)](https://opensource.org/licenses/MIT)

This is an asynchronous implementation of a Logback appender that writes events to a specified Amazon CloudWatch log group.  It requires Java 8+ to compile.

The logger will fail if the matching log group is not found, but will attempt to create a missing log stream within the group. This is designed to allow the creation of per-instance streams based on dynamic parameters inside of a cluster.  You could accomplish this by either supplying a property inside your log definition or using a `streamNameSupplier` to inject the correct stream at generation time.

### Configuration Parameters
| Name               | Type               | Purpose                                   | Required? |
|:-------------------|:-------------------|:------------------------------------------|:---------:|
| encoder            | [Logback Encoder](https://logback.qos.ch/manual/encoders.html) | describes a format for logged messages | yes |
| groupName          | `String`            | name of the CloudWatch log group         | yes       |
| streamName         | `String`            | name of the CloudWatch log stream        | no^       |
| streamNameSupplier | `Supplier<String>`  | source for CloudWatch log stream names   | no^       |
| awsLogsSupplier    | `Supplier<AWSLogs>` | source for a AWSLogs (CloudWatch) client | no        |

^ either a `streamName` or a `streamNameSupplier` must be provided

### Sample Configuration
```xml
<appender name="AWS-CLOUD-WATCH" class="com.xpcagey.logback.CloudWatchAppender">
    <encoder>
        <pattern>%-5level [%.15thread] %logger{0}: %msg%n</pattern>
    </encoder>
    <groupName>MyGroup</groupName>
    <streamName>MyStream</streamName>
</appender>
```

Note that the timestamp is not present here because it is logged as an external field by CloudWatch. An example of a full file can be found in the integration tests [here](src/test/resources/logback-test.xml)

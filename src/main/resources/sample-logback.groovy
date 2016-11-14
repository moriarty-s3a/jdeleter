import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender

import static ch.qos.logback.classic.Level.INFO

appender('file', FileAppender) {
    file = '/var/log/jdeleter.log'
    append = true
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} %-5level [%thread] - %msg%n"
    }
}

logger("org.s3a.deleter", INFO, ['file'])
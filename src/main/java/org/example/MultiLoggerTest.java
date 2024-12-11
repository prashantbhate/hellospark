package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MultiLoggerTest {

    // SLF4J Logger
    private static final Logger slf4jLogger = LoggerFactory.getLogger(MultiLoggerTest.class);

    // Commons Logging Logger
    private static final Log commonsLogger = LogFactory.getLog(MultiLoggerTest.class);

    // JUL Logger
    private static final java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(MultiLoggerTest.class.getName());

    public static void main(String[] args) {

        // SLF4J logging
        slf4jLogger.debug("SLF4J log message.");
        slf4jLogger.info("SLF4J log message.");
        slf4jLogger.warn("SLF4J warning message.");

        // Commons Logging
        commonsLogger.debug("Commons Logging warning message.");
        commonsLogger.info("Commons Logging log message.");
        commonsLogger.warn("Commons Logging warning message.");

        // JUL logging
        julLogger.fine("JUL log message.");
        julLogger.info("JUL log message.");
        julLogger.warning("JUL warning message.");

        // Log4j 2 logging (this is being routed by SLF4J)
        org.apache.logging.log4j.LogManager.getLogger(MultiLoggerTest.class).info("Log4j 2 log message.");
        org.apache.logging.log4j.LogManager.getLogger(MultiLoggerTest.class).warn("Log4j 2 warning message.");
    }
}

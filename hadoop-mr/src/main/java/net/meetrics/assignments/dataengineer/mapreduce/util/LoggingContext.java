package net.meetrics.assignments.dataengineer.mapreduce.util;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * @author meetrics.com
 */
public class LoggingContext {

    private static boolean configured = false;

    public static void configureWithAppender() {
        if (! configured) {
            BasicConfigurator.configure();
            configure();
        }
        configured = true;
    }

    private static void configure() {
        if (! configured) {
            LogManager.getRootLogger().setLevel(Level.INFO);
        }
        configured = true;
    }
}

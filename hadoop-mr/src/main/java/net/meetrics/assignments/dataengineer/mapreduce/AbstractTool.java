package net.meetrics.assignments.dataengineer.mapreduce;

import net.meetrics.assignments.dataengineer.mapreduce.util.LoggingContext;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * @author meetrics.com
 */
public abstract class AbstractTool extends Configured implements Tool {

    static {
        LoggingContext.configureWithAppender();
    }

    @Override
    public abstract int run(final String[] args) throws Exception;
}

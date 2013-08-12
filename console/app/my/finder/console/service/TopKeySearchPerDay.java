package my.finder.console.service;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;

@DisallowConcurrentExecution
public class TopKeySearchPerDay implements Job {
    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        java.util.Calendar ctime = java.util.Calendar.getInstance();
        ctime.setTime( new Date() );
        ctime.add(java.util.Calendar.DATE, -1);
        SummarizingService.paymentTopKey( ctime );
    }

}


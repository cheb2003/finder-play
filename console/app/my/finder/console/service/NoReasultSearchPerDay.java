package my.finder.console.service;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;


public class NoReasultSearchPerDay implements Job {
    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        java.util.Calendar calend = java.util.Calendar.getInstance();
        calend.setTime(context.getFireTime());
        calend.add(java.util.Calendar.DATE, -1);

        KPIService.searchNoResult(calend);
    }

}
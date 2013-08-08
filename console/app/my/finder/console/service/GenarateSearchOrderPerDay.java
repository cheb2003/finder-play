package my.finder.console.service;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@DisallowConcurrentExecution
public class GenarateSearchOrderPerDay implements Job {
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        java.util.Calendar calend = java.util.Calendar.getInstance();
        calend.setTime(context.getFireTime());
        calend.add(java.util.Calendar.DATE, -1);
        KPIService.paymentOrder(calend);
    }
}


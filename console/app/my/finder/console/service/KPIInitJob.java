package my.finder.console.service;

import org.quartz.*;

public class KPIInitJob {
    public static void init(Scheduler scheduler) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(TopKeySearchPerDay.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 1 0 * * ?")).build();
        scheduler.scheduleJob(jobDetail,trigger);
    }
}

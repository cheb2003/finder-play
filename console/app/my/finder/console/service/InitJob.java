package my.finder.console.service;


import org.quartz.*;

/**
 *

 */
public class InitJob {
    public static void init(Scheduler scheduler) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(GenarateSearchOrderPerDay.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 30 0 * * ?")).build();
        scheduler.scheduleJob(jobDetail,trigger);

        JobDetail jobDetail2 = JobBuilder.newJob(TopKeySearchPerDay.class).build();
        Trigger trigger2 = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 1 0 * * ?")).build();
        scheduler.scheduleJob(jobDetail2,trigger2);

        JobDetail jobDetail3 = JobBuilder.newJob(TopKeySearchPerDay.class).build();
        Trigger trigger3 = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 1 0 * * ?")).build();
        scheduler.scheduleJob(jobDetail3,trigger3);
    }
}

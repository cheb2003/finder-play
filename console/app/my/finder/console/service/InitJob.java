package my.finder.console.service;


import org.quartz.*;

/**
 *

 */
public class InitJob {
    public static void init(Scheduler scheduler) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(TopKeySearchPerDay.class).build();
        Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("*/10 * * * * ?")).build();
        scheduler.scheduleJob(jobDetail,trigger);
    }
}

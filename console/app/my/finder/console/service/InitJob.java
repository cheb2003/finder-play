package my.finder.console.service;


import org.quartz.*;


public class InitJob {
    public static void init(Scheduler scheduler) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(TopKeySearchPerDay.class).build();
        JobDetail jobDetail2 = JobBuilder.newJob(GenarateSearchOrderPerDay.class).build();
        Trigger trigger1 = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("5 1 3 * * ?")).build();
        Trigger trigger2 = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("5 1 2 * * ?")).build();
        scheduler.scheduleJob(jobDetail,trigger1);
        scheduler.scheduleJob(jobDetail2,trigger2);
    }
}

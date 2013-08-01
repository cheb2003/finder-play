package my.finder.console.service;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-7-26
 * Time: 上午9:43
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    /*public static void main(String[] args) throws SchedulerException,
            ParseException {
        Scheduler handsomeMan = new StdSchedulerFactory().getScheduler();
        JobDetail conn = new JobDetail("搜索订单查询", "query1",
                GenarateSearchOrderPerDay.class);

        CronTrigger t = new CronTrigger("trigger", "query1",
                "0 24 13 * * ? * ");

        long startTime = System.currentTimeMillis();
        SimpleTrigger momentTrigger2 = new SimpleTrigger("trigger2", "query1");
        momentTrigger2.setStartTime(new Date(startTime));
        momentTrigger2.setEndTime(new Date(startTime + 6000L));
        momentTrigger2.setRepeatCount(10);
        momentTrigger2.setRepeatInterval(1000L);

        handsomeMan.scheduleJob(concerneGirl, momentTrigger);
        handsomeMan.scheduleJob(conn, t);

        handsomeMan.start();
    }*/


}

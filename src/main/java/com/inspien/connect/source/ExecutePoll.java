package com.inspien.connect.source;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ExecutePoll implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        ((Thread) jobDataMap.get("Thread")).interrupt();
        System.out.println("하이");
    }
}

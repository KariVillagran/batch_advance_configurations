package com.duoc.batch_demo;

import com.duoc.advanced.JobRunner;
import com.duoc.advanced.VentasJobConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = "com.duoc.advanced")
public class JobRunnerMain {

    public static void main(String[] args) throws Exception {
        ApplicationContext context = new AnnotationConfigApplicationContext(VentasJobConfig.class);

        JobRunner jobRunner = context.getBean(JobRunner.class);

        jobRunner.runVentasJob();
        
        ((AnnotationConfigApplicationContext) context).close();
    }
}
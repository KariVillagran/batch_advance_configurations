package com.duoc.batch_demo;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import com.duoc.advanced.JobRunner;
import com.duoc.advanced.VentasJobConfig;

public class JobRunnerMain {

    public static void main(String[] args) {
        // Inicia el contexto de Spring
        ApplicationContext context = new AnnotationConfigApplicationContext(VentasJobConfig.class);

        // Obtén el bean JobRunner desde el contexto
        JobRunner jobRunner = context.getBean(JobRunner.class);

        // Ejecuta el job
        jobRunner.runVentasJob();
        
        // Cierra el contexto
        ((AnnotationConfigApplicationContext) context).close();
    }
}
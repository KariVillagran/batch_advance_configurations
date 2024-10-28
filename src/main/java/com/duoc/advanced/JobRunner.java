package com.duoc.advanced;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job ventasJob;

    public void runVentasJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis()) // Parámetro único para asegurar que cada ejecución sea única
                    .toJobParameters();
            jobLauncher.run(ventasJob, jobParameters);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
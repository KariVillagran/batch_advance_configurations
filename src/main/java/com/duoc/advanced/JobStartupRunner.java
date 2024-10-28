package com.duoc.advanced;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class JobStartupRunner {

    @Autowired
    private JobRunner jobRunner;

    @EventListener(ApplicationReadyEvent.class)
    public void runJobAtStartup() {
        jobRunner.runVentasJob();
    }
}

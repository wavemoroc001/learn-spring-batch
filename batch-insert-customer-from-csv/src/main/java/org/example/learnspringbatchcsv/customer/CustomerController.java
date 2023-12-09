package org.example.learnspringbatchcsv.customer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@RestController
@Slf4j
public class CustomerController {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job job;

    @GetMapping("/initialize")
    public ResponseEntity<BatchStatus> initializeJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, JobParameter<?>> maps = new HashMap<>();
        maps.put("batchStartDate", new JobParameter<>(System.currentTimeMillis(), Long.class));

        JobParameters parameters = new JobParameters(maps);
        JobExecution execution = jobLauncher.run(job, parameters);
        log.info("Job started at {}", LocalDate.now());
        while (execution.isRunning()) {
            log.info("job is running...");
        }
        return ResponseEntity.ok(execution.getStatus());
    }
}

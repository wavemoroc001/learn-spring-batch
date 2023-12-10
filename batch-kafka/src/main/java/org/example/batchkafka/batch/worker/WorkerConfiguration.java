package org.example.batchkafka.batch.worker;

import lombok.RequiredArgsConstructor;
import org.example.batchkafka.customer.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class WorkerConfiguration {

    private final KafkaProperties kafkaProperties;

    @Bean
    public KafkaItemReader<String, Customer> InboundWorkerReader(KafkaTemplate<String, Customer> kafkaTemplate) {
        var properties = new Properties();
        properties.putAll(this.kafkaProperties.buildConsumerProperties());
        return new KafkaItemReaderBuilder<String, Customer>()
                .topic("customers")
                .partitions(1)
                .consumerProperties(properties)
                .name("customer-reader")
                .saveState(true)
                .build();
    }

    @Bean
    public KafkaItemWriter<String, Customer> OutboundWorkerWriter(KafkaTemplate<String, Customer> kafkaTemplate) {
        return new KafkaItemWriterBuilder<String, Customer>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(new Converter<Customer, String>() {
                    @Override
                    public String convert(Customer source) {
                        return source.getEmail() + "-" + "success";
                    }
                })
                .build();
    }

    @Bean
    public Step inboundWorkerStep(PlatformTransactionManager transactionManager,
                                  @Qualifier(value = "InboundWorkerReader") KafkaItemReader itemReader,
                                  ItemProcessor itemProcessor,
                                  @Qualifier(value = "OutboundWorkerWriter") KafkaItemWriter kafkaItemWriter,
                                  JobRepository jobRepository) {
        return new StepBuilder("workerStep", jobRepository)
                .chunk(10, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(kafkaItemWriter)
                .build();
    }

    @Bean
    public Job workerInboundJob(@Qualifier(value = "inboundWorkerStep") Step inboundStep,
                                JobRepository jobRepository) {
        return new JobBuilder("workerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(inboundStep)
                .build();
    }
}

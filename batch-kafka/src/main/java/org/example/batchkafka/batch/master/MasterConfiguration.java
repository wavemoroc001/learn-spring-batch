package org.example.batchkafka.batch.master;

import org.example.batchkafka.customer.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Configuration
public class MasterConfiguration {

    @Bean
    public KafkaItemWriter<String, Customer> kafkaItemWriter(KafkaTemplate<String, Customer> kafkaTemplate) {
        return new KafkaItemWriterBuilder<String, Customer>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(new Converter<Customer, String>() {
                    @Override
                    public String convert(Customer source) {
                        return LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))+ "-" + source.getEmail();
                    }
                })
                .build();
    }

    // read customer from csv
    // and produce to kafka
    @Bean
    public Step outboundWorkerStep(PlatformTransactionManager transactionManager,
                                   @Qualifier(value = "fileItemReader") ItemReader itemReader,
                                   ItemProcessor itemProcessor,
                                   KafkaItemWriter kafkaItemWriter,
                                   JobRepository jobRepository) {
        return new StepBuilder("produce-customer-step", jobRepository)
                .chunk(10, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(kafkaItemWriter)
                .build();
    }

    @Bean
    public Job outboundWorkerJob(JobRepository jobRepository,
                                 @Qualifier(value = "outboundWorkerStep") Step outboundStep) {
        return new JobBuilder("master-produce-customer-to-worker", jobRepository)
                .start(outboundStep)
                .build();
    }


    // Item Reader
    @Bean
    public FlatFileItemReader<Customer> fileItemReader(@Value("${input}") Resource resource) {
        FlatFileItemReader<Customer> fileItemReader = new FlatFileItemReader<>();
        // set path to read csv file
        fileItemReader.setResource(resource);
        fileItemReader.setName("CSV-Reader");
        // skip csv header
        fileItemReader.setLinesToSkip(1);
        fileItemReader.setLineMapper(lineMapper());
        return fileItemReader;
    }

    @Bean
    public LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "first_name", "last_name", "email", "gender");

        // set pojo class to map csv
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);
        return lineMapper;
    }
}

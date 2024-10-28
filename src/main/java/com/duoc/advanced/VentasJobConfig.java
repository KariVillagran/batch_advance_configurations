package com.duoc.advanced;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
@EnableBatchProcessing
@Import(DataSourceConfiguration.class)
public class VentasJobConfig {

    @Bean
    public Job ventasJob(JobRepository jobRepository, Step step) {
        return new JobBuilder("ventasJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step consolidacionDiariaStep(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                     FlatFileItemReader<Venta> itemReader, VentasItemProcessor itemProcessor,
                     FlatFileItemWriter<Venta> itemWriter) {
        return new StepBuilder("consolidacionDiariaStep", jobRepository)
                .<Venta, Venta>chunk(10, transactionManager)
                .reader(ventasItemReader())
                .processor(ventasItemProcessor())
                .writer(ventasItemWriter())
                .build();
    }

    @Bean
    public FlatFileItemReader<Venta> ventasItemReader() {
        return new FlatFileItemReaderBuilder<Venta>()
                .name("ventasItemReader")
                .resource(new FileSystemResource("consolidacion_diaria_ventas.csv")) // Ruta del archivo de entrada
                .delimited()
                .names("id", "producto", "cantidad", "precio")
                .targetType(Venta.class)
                .build();
    }

    @Bean
    public VentasItemProcessor ventasItemProcessor() {
        return new VentasItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<Venta> ventasItemWriter() {
        // Configuración del escritor para escribir el archivo CSV de salida
        FlatFileItemWriter<Venta> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));

        // Definir el formato de escritura
        DelimitedLineAggregator<Venta> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        // Definir el mapeo de campos
        BeanWrapperFieldExtractor<Venta> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"producto", "cantidad", "precio"});
        lineAggregator.setFieldExtractor(fieldExtractor);

        writer.setLineAggregator(lineAggregator);
        return writer;
    }
}
package com.duoc.advanced;

import javax.sql.DataSource;
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
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class VentasJobConfig {

    private final JobRepository jobRepository;
    private final DataSource dataSource;

    public VentasJobConfig(JobRepository jobRepository, DataSource dataSource) {
        this.jobRepository = jobRepository;
        this.dataSource = dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public Job ventasJob() {
        return new JobBuilder("ventasJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(consolidacionDiariaStep())
                .build();
    }

    @Bean
    public Step consolidacionDiariaStep() {
        return new StepBuilder("consolidacionDiariaStep", jobRepository)
                .<Venta, InformeVenta>chunk(10, transactionManager())
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
    public FlatFileItemWriter<InformeVenta> ventasItemWriter() {
        return new FlatFileItemWriterBuilder<InformeVenta>()
                .name("ventasItemWriter")
                .resource(new FileSystemResource("consolidacion_ventas.csv")) // Ruta del archivo de salida
                .lineAggregator(new DelimitedLineAggregator<InformeVenta>() {{
                    setDelimiter(",");
                    setFieldExtractor(new FieldExtractor<InformeVenta>() {
                        @Override
                        public Object[] extract(InformeVenta item) {
                            return new Object[]{item.getProducto(), item.getCantidadTotal(), item.getTotalVentas()};
                        }
                    });
                }})
                .build();
    }
}
package com.duoc.advanced;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;


@Configuration
@EnableBatchProcessing
@Import(DataSourceConfiguration.class)
public class VentasJobConfig {
    private static final Logger logger = LoggerFactory.getLogger(VentasJobConfig.class);

    // Configuración del Job con ID incremental y Decisor para manejo de finalización
    @Bean
    public Job ventasJob(JobRepository jobRepository, Step step, JobExecutionDecider decider, JobCompletionListener listener) {
        return new JobBuilder("ventasJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener) // Registra el listener aquí
                .start(step)
                .next(decider) // Usa el decisor para manejar la lógica de flujo
                .on("COMPLETED").end() // Cierra el flujo si el paso se completa
                .from(decider).on("RETRY").to(step) // En caso de "RETRY", vuelve a intentar el paso
                .end() // Termina la configuración del flujo de trabajo
                .build(); // Devuelve el Job
    }

    // Step configurado para procesamiento paralelo, reintentos y política de repetición
    @Bean
    public Step consolidacionDiariaStep(JobRepository jobRepository, 
                                        JdbcTransactionManager transactionManager,
                                        FlatFileItemReader<Venta> itemReader, 
                                        VentasItemProcessor itemProcessor,
                                        FlatFileItemWriter<InformeVenta> itemWriter,
                                        FlatFileItemWriter<Venta> errorItemWriter,
                                        ErrorFileStepExecutionListener errorFileStepExecutionListener 
                                        // ThreadPoolTaskExecutor taskExecutor,
                                        ) {
        return new StepBuilder("consolidacionDiariaStep", jobRepository)
                .<Venta, InformeVenta>chunk(new SimpleCompletionPolicy(2), transactionManager) // Usa CompletionPolicy
                .reader(ventasItemReader())
                .processor(ventasItemProcessor())
                .writer(ventasItemWriter())
                .faultTolerant()
                .skipPolicy(customSkipPolicy())
                .skip(InvalidDataException.class)
                .skipLimit(10)
                .skipPolicy(new FileVerificationSkipper()) // Política de salto para errores en lectura
                //.retryLimit(3) // Límite de reintentos para errores transitorios
                //.retry(Exception.class) // Define excepciones para reintentar
                .listener(errorFileStepExecutionListener) // Añade el StepExecutionListener
                .listener(skipListener(errorItemWriter))  // Añade el SkipListener para escribir errores                
                //.taskExecutor(taskExecutor) // Configura la ejecución en paralelo
                .build();
    }

    // Configura un lector de archivo CSV como en el ejemplo original
    @Bean
    public FlatFileItemReader<Venta> ventasItemReader() {
        FlatFileItemReader<Venta> reader = new FlatFileItemReaderBuilder<Venta>()
                .name("ventasItemReader")
                .resource(new ClassPathResource("consolidacion_diaria_ventas.csv"))
                .linesToSkip(1)
                .delimited()
                .names("id", "producto", "cantidad", "precio")
                .targetType(Venta.class)
                .build();
        // Maneja excepciones de parseo y registra el error
        reader.setStrict(false); // Permite continuar en caso de error
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Venta> errorItemWriter() {
        FlatFileItemWriter<Venta> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("errores.csv"));

        DelimitedLineAggregator<Venta> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<Venta> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "producto", "cantidad", "precio"});
        lineAggregator.setFieldExtractor(fieldExtractor);

        writer.setLineAggregator(lineAggregator);
        return writer;
    }

    @Bean
    public SkipPolicy customSkipPolicy() {
        return (Throwable t, long skipCount) -> {
            logger.warn("CustomSkipPolicy - Excepción omitida: {}", t.getMessage());
            if (t instanceof InvalidDataException && skipCount < 10) {
                logger.warn("CustomSkipPolicy - Excepción omitida: {}", t.getMessage());
                return true;  // Indica que la excepción se debe omitir
            }
            return false;
        };
    }

    @Bean
    public SkipListener<Venta, InformeVenta> skipListener(FlatFileItemWriter<Venta> errorItemWriter) {
        logger.warn("Configurando SkipListener...");
        
        return new SkipListener<Venta, InformeVenta>() {  // Tipos explícitos
            @Override
            public void onSkipInProcess(Venta item, Throwable t) {
                logger.info("Tipo de excepción en SkipListener: {}", t.getClass().getName());
                logger.warn("SkipListener activado - Error al procesar registro: {}", item);
                try {
                    // Escribe el registro en el archivo de errores directamente
                    errorItemWriter.write(new Chunk<>(List.of(item)));
                    logger.info("Registro escrito en errores.csv: {}", item);
                } catch (Exception e) {
                    logger.error("Error al escribir registro omitido en archivo de errores: ", e);
                }
            }
    
            @Override
            public void onSkipInRead(Throwable t) {
                if (t instanceof FlatFileParseException) {
                    FlatFileParseException ffpe = (FlatFileParseException) t;
                    logger.warn("Línea omitida debido a un error: {}", ffpe.getInput());
                }
            }
    
            @Override
            public void onSkipInWrite(InformeVenta item, Throwable t) {
                logger.error("Error al escribir registro: ", t);
            }
        };
    }

    // Procesador de ítems que transforma datos de venta en informes de venta
    @Bean
    public VentasItemProcessor ventasItemProcessor() {
        return new VentasItemProcessor();
    }

    // Configura un escritor de archivo CSV con un encabezado
    @Bean
    public FlatFileItemWriter<InformeVenta> ventasItemWriter() {
        FlatFileItemWriter<InformeVenta> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));

        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("Producto,Cantidad Total,Total Ventas");
            }
        });

        DelimitedLineAggregator<InformeVenta> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<InformeVenta> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"producto", "cantidadTotal", "totalVentas"});
        lineAggregator.setFieldExtractor(fieldExtractor);

        writer.setLineAggregator(lineAggregator);
        return writer;
    }

    @Bean
    public JobExecutionDecider decider() {
        return new CustomDecider(); // Registra el decider personalizado
    }

    // Configura el TaskExecutor para procesamiento paralelo
    /*@Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5); // Tamaño inicial del pool de hilos
        executor.setMaxPoolSize(10); // Tamaño máximo de hilos
        executor.setQueueCapacity(25); // Capacidad de la cola de espera
        executor.setThreadNamePrefix("Batch-Thread-");
        executor.initialize();
        return executor;
    }*/
}
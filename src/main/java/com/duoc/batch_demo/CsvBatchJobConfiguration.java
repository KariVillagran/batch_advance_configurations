package com.duoc.batch_demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
@EnableBatchProcessing
@Import(DataSourceConfiguration.class)
public class CsvBatchJobConfiguration  {

    @Bean
    public FlatFileItemReader<Person> itemReader() {
        // Configuración del lector para leer el archivo CSV
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("data.csv"));
        reader.setLinesToSkip(1); // Saltar la primera línea (cabecera)

        // Definir el mapeo de cada línea del archivo CSV a un objeto Person
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("id", "name", "age");

        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Person.class);

        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    public ItemProcessor<Person, Person> itemProcessor() {
        // Procesador que transforma el nombre a mayúsculas
        return person -> {
            person.setName(person.getName().toUpperCase()); // Transformar el nombre a mayúsculas
            return person;
        };
    }

    @Bean
    public FlatFileItemWriter<Person> itemWriter() {
        // Configuración del escritor para escribir el archivo CSV de salida
        FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));

        // Definir el formato de escritura
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        // Definir el mapeo de campos
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "age"});
        lineAggregator.setFieldExtractor(fieldExtractor);

        writer.setLineAggregator(lineAggregator);
        return writer;
    }

    @Bean
    public Step step(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                     FlatFileItemReader<Person> itemReader, ItemProcessor<Person, Person> itemProcessor,
                     FlatFileItemWriter<Person> itemWriter) {
        // Configuración del Step utilizando el nuevo método chunk
        return new StepBuilder("csvStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager) // Especifica el tamaño del chunk y el transactionManager
                .reader(itemReader)
                .processor(itemProcessor) // Procesador que convierte el nombre a mayúsculas
                .writer(itemWriter) // Escritor que guarda los datos en un nuevo archivo CSV
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        // Configuración del Job que contiene el Step
        return new JobBuilder("csvJob", jobRepository)
                .start(step)
                .build();
    }

    // Método main de la aplicación
    public static void main(String[] args) throws Exception {
        /*
        * Crea un contexto de aplicación basado en anotaciones, cargando la configuración
        * especificada en la clase CsvBatchJobConfiguration.
        */
        ApplicationContext context = new AnnotationConfigApplicationContext(CsvBatchJobConfiguration.class);

        /* 
        * Obtiene el bean de JobLauncher desde el contexto. El JobLauncher se encarga de ejecutar
        * trabajos batch configurados en Spring Batch.
        */
        JobLauncher jobLauncher = context.getBean(JobLauncher.class);

        /* 
        * Obtiene el bean del Job (trabajo batch) desde el contexto. El Job representa el proceso
        * batch que se va a ejecutar.
        */
        Job job = context.getBean(Job.class);

        /*
        * Ejecuta el trabajo batch utilizando el JobLauncher, pasando el Job y los parámetros
        * necesarios para su ejecución (en este caso, un conjunto vacío de parámetros).
        */
        jobLauncher.run(job, new JobParameters());
    }

}
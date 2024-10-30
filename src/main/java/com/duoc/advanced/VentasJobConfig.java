package com.duoc.advanced;

import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

/**
 * Configuración del trabajo Spring Batch para procesar ventas.
 * Define el flujo de trabajo, incluyendo pasos de lectura, procesamiento y escritura de archivos.
 */
@Configuration
@EnableBatchProcessing
@Import(DataSourceConfiguration.class) // Importa la configuración de datos externa
public class VentasJobConfig {

    /**
     * Define el trabajo principal de Spring Batch, que incluye el identificador
     * y el paso inicial.
     *
     * @param jobRepository El repositorio de trabajos utilizado para gestionar la ejecución del trabajo.
     * @param step El paso de consolidación de datos diarios.
     * @return El trabajo configurado para procesamiento de ventas.
     */
    @Bean
    public Job ventasJob(JobRepository jobRepository, Step step) {
        return new JobBuilder("ventasJob", jobRepository)
                .incrementer(new RunIdIncrementer()) // Incrementa el ID del trabajo para cada ejecución
                .start(step) // Inicia con el paso especificado
                .build();
    }

    /**
     * Define el paso del trabajo que realiza la consolidación diaria de ventas.
     * Este paso incluye la lectura de datos, el procesamiento y la escritura en un archivo.
     *
     * @param jobRepository Repositorio para gestionar el estado del paso.
     * @param transactionManager Manejador de transacciones para garantizar la atomicidad.
     * @param itemReader Lector de archivos que extrae los datos de entrada.
     * @param itemProcessor Procesador que transforma los datos de entrada en un informe.
     * @param itemWriter Escritor de archivos que exporta los datos procesados.
     * @return El paso configurado.
     */
    @Bean
    public Step consolidacionDiariaStep(JobRepository jobRepository, JdbcTransactionManager transactionManager,
                                        FlatFileItemReader<Venta> itemReader, VentasItemProcessor itemProcessor,
                                        FlatFileItemWriter<InformeVenta> itemWriter) {
        return new StepBuilder("consolidacionDiariaStep", jobRepository)
                .<Venta, InformeVenta>chunk(10, transactionManager) // Tamaño del lote de procesamiento
                .reader(ventasItemReader())
                .processor(ventasItemProcessor())
                .writer(ventasItemWriter())
                .build();
    }

    /**
     * Configura el lector de archivos para leer un archivo CSV de ventas.
     * Define los nombres de columnas y la estructura del archivo.
     *
     * @return El lector configurado para archivos CSV.
     */
    @Bean
    public FlatFileItemReader<Venta> ventasItemReader() {
        return new FlatFileItemReaderBuilder<Venta>()
                .name("ventasItemReader")
                .resource(new ClassPathResource("consolidacion_diaria_ventas.csv")) // Ruta del archivo de entrada
                .linesToSkip(1) // Saltar la primera línea (cabecera)
                .delimited()
                .names("id", "producto", "cantidad", "precio") // Campos del archivo CSV
                .targetType(Venta.class)
                .build();
    }

    /**
     * Define el procesador que convierte una venta en un informe de venta.
     * Este procesador realiza cálculos sobre los datos de la venta.
     *
     * @return El procesador configurado.
     */
    @Bean
    public VentasItemProcessor ventasItemProcessor() {
        return new VentasItemProcessor();
    }

    /**
     * Configura el escritor de archivos para escribir los informes de venta en un archivo CSV.
     * Incluye la definición de un encabezado y el formato de delimitador para los datos.
     *
     * @return El escritor configurado para archivos CSV.
     */
    @Bean
    public FlatFileItemWriter<InformeVenta> ventasItemWriter() {
        FlatFileItemWriter<InformeVenta> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));

        // Agrega un encabezado al archivo CSV de salida
        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("Producto,Cantidad Total,Total Ventas"); // Escribe el encabezado
            }
        });

        // Configuración de agregador de líneas
        DelimitedLineAggregator<InformeVenta> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        // Configuración del extractor de campos
        BeanWrapperFieldExtractor<InformeVenta> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"producto", "cantidadTotal", "totalVentas"});
        lineAggregator.setFieldExtractor(fieldExtractor);

        writer.setLineAggregator(lineAggregator);
        return writer;
    }
}

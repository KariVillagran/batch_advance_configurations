package com.duoc.advanced;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.stereotype.Component;

@Component
public class ErrorFileStepExecutionListener implements StepExecutionListener {

    private final FlatFileItemWriter<Venta> errorItemWriter;

    public ErrorFileStepExecutionListener(FlatFileItemWriter<Venta> errorItemWriter) {
        this.errorItemWriter = errorItemWriter;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        errorItemWriter.open(new ExecutionContext()); // Abre el archivo de errores al inicio
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        errorItemWriter.close(); // Cierra el archivo de errores al final
        return ExitStatus.COMPLETED;
    }
}
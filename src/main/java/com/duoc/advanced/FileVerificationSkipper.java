package com.duoc.advanced;

import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

public class FileVerificationSkipper implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) { // Cambia int a long en skipCount
        // Omite líneas si el error es FlatFileParseException y el número de saltos no supera el límite
        return t instanceof FlatFileParseException && skipCount <= 10; // Máximo de 10 saltos permitidos
    }
}
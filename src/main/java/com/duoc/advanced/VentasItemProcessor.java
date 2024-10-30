package com.duoc.advanced;

import org.springframework.batch.item.ItemProcessor;

/**
 * Procesador de items en Spring Batch que convierte un objeto de tipo Venta en un objeto de tipo InformeVenta.
 * Este procesador realiza la transformación de los datos de cada venta, calculando el total de ventas
 * basado en la cantidad y el precio de cada producto.
 */
public class VentasItemProcessor implements ItemProcessor<Venta, InformeVenta> {

    /**
     * Procesa un objeto Venta para generar un objeto InformeVenta.
     * Calcula el total de ventas multiplicando la cantidad por el precio del producto.
     *
     * @param venta Objeto Venta que contiene los datos de entrada.
     * @return Un nuevo objeto InformeVenta con los datos procesados.
     * @throws Exception Si ocurre un error durante el procesamiento.
     */
    @Override
    public InformeVenta process(Venta venta) throws Exception {
        InformeVenta informe = new InformeVenta();
        
        // Establece el nombre del producto en el informe
        informe.setProducto(venta.getProducto());
        
        // Establece la cantidad total del producto en el informe
        informe.setCantidadTotal(venta.getCantidad());
        
        // Calcula y establece el total de ventas multiplicando cantidad y precio
        informe.setTotalVentas(venta.getCantidad() * venta.getPrecio());
        
        return informe;
    }
}

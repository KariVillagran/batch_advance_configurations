package com.duoc.advanced;

import org.springframework.batch.item.ItemProcessor;

public class VentasItemProcessor implements ItemProcessor<Venta, InformeVenta> {
    @Override
    public InformeVenta process(Venta venta) throws Exception {
        InformeVenta informe = new InformeVenta();
        informe.setProducto(venta.getProducto());
        informe.setCantidadTotal(venta.getCantidad());
        informe.setTotalVentas(venta.getCantidad() * venta.getPrecio());
        return informe;
    }
}
package com.duoc.advanced;

import org.springframework.batch.item.ItemProcessor;

public class VentasItemProcessor implements ItemProcessor<Venta, Venta> {
    @Override
    public Venta process(Venta venta) throws Exception {
        Venta informe = new Venta();
        informe.setProducto(venta.getProducto());
        informe.setCantidad(venta.getCantidad());
        informe.setPrecio(venta.getCantidad() * venta.getPrecio());
        return informe;
    }
}
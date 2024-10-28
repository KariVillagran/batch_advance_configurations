package com.duoc.advanced;

public class Venta {
    private Long id;
    private String producto;
    private int cantidad;
    private double precio;

    public Venta() {}

    public Long getId() {
        return id;
    }
    public String getProducto() {
        return producto;
    }
    public int getCantidad() {
        return cantidad;
    }
    public double getPrecio() {
        return precio;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public void setProducto(String producto) {
        this.producto = producto;
    }
    public void setCantidad(int cantidad) {
        this.cantidad = cantidad;
    }
    public void setPrecio(double precio) {
        this.precio = precio;
    }

    
    
}

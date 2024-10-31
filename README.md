# Proyecto Spring Batch - Configuraciones avanzadas de Spring batch
Este proyecto es una solución de procesamiento de datos con Spring Batch para transformar archivos CSV de ventas en informes consolidados. Utiliza un flujo de lectura, validación y transformación de datos para procesar cada venta, generando un archivo de salida con totales por producto y un archivo de errores con registros que no cumplen los criterios. La configuración de tolerancia a fallos y la ejecución en paralelo permiten un procesamiento eficiente de grandes volúmenes de datos, asegurando precisión y reportando errores en cada ejecución.

## Requisitos previos
- **Java 21**: Asegúrate de tener instalado JDK 21.
- **Maven 3.x** o superior: Para compilar y ejecutar el proyecto.

## Tecnologías utilizadas
- **Java 21**
- **Spring Batch**
- **Spring Boot**
- **Maven**

# Como utilizar
Para poder utilizar este repositorio, deberas abrir tu terminal (bash/PowerShell) e ir al directorio del proyecto.

1. Clonar el repositorio

```bash
git clone https://github.com/KariVillagran/batch_advance_configurations.git
cd batch_advance_configurations
```

2. Compila y ejecuta

```bash
mvn clean install
mvn spring-boot:run
```

Alternativamente, también puedes ejecutar el proyecto directamente usando el comando:

```bash
java -jar target/batch-advance-configurations-0.0.1-SNAPSHOT.jar
```
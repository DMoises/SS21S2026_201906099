# DOCUMENTACIÓN TÉCNICA: PROYECTO 1
**Implementación de Flujo Completo Microsoft BI (SSIS, SSAS, SQL Server)**
**Empresa:** SGFood  |  **Área:** Ventas e Inventarios

### 1. Arquitectura General de la Solución
Se diseñó e implementó una arquitectura de Business Intelligence (BI) basada en el ecosistema de Microsoft. El flujo de datos (Pipeline) integra información heterogénea proveniente de un sistema transaccional (OLTP) alojado en la nube (SQL Server - IP `34.63.26.98`) y un archivo plano complementario (`Excel`). La arquitectura consta de tres capas principales:
1.  **Capa de Integración (SSIS):** Orquestación del proceso ETL (Extracción, Transformación y Carga).
2.  **Capa de Almacenamiento Analítico (SQL Server Local):** Repositorio centralizado estructurado como Data Warehouse.
3.  **Capa Semántica y de Consumo (SSAS):** Cubo OLAP multidimensional para consultas de alto rendimiento y navegación jerárquica.

### 2. Diseño del Data Warehouse (Modelo Dimensional)
Se optó por implementar un **Modelo en Estrella (Star Schema)**, siguiendo las metodologías de Ralph Kimball. Esta decisión arquitectónica se justifica por su alto rendimiento en consultas de agregación masiva y su integración nativa con herramientas de Analysis Services.
* **Granularidad:** El grano de la tabla de hechos (`FactVentasInventario`) se definió a nivel de transacción individual (una fila por cada venta de un producto en un día específico a un cliente específico).
* **Dimensiones Conformadas:** Se crearon 4 dimensiones descriptivas (`DimProducto`, `DimCliente`, `DimCanal`, `DimFecha`). Para aislar el Data Warehouse de los cambios en los sistemas transaccionales, se implementaron *Surrogate Keys* (Llaves Subrogadas numéricas autoincrementales) en lugar de utilizar las *Natural Keys* (como el SKU o el ID de Cliente).
* **Integridad y Rendimiento:** Se establecieron restricciones de clave foránea (Foreign Keys) estrictas y se crearon índices *Non-Clustered* en las llaves foráneas de la tabla de hechos para acelerar los JOINs durante el procesamiento del cubo.

### 3. Fase de Extracción, Transformación y Carga (ETL)
El paquete de SQL Server Integration Services (`SGFood_ETL_Proyecto1`) se estructuró dividiendo el Flujo de Control en contenedores lógicos para asegurar el orden de ejecución (Dimensiones primero, Hechos después) previniendo bloqueos de integridad referencial.
* **Limpieza y Estandarización:** Se utilizaron componentes `Data Conversion` para resolver conflictos de codificación (transformando tipos `DT_WSTR` de Excel a `DT_STR` del motor relacional). Los valores nulos y cálculos de métricas (como Importe Neto y Margen Estimado) se resolvieron "en vuelo" utilizando el componente `Derived Column`.
* **Generación de la Dimensión de Tiempo:** Se aplicó lógica T-SQL en la extracción para generar una *Smart Key* de fecha (formato `YYYYMMDD`), estándar en la industria para dimensiones temporales.
* **Mapeo de Llaves (Lookups):** Para la carga de la tabla de hechos, se implementó un patrón de búsqueda utilizando componentes `Lookup`, cruzando las llaves naturales provenientes del origen transaccional con las tablas dimensionales locales para extraer e inyectar las llaves subrogadas correctas.
* **Integración Heterogénea:** Para cumplir con la ingesta del archivo Excel, se utilizó un componente `OLE DB Command` que ejecutó sentencias `UPDATE` parametrizadas, enriqueciendo la tabla `DimProducto` con el campo `Fabricante`.

### 4. Modelo Analítico Multidimensional (SSAS)
Se construyó un Cubo OLAP (`SGFood_Cubo_Proyecto1`) para habilitar el análisis gerencial.
* **Medidas y Dimensiones:** Se consolidó `FactVentasInventario` como el *Measure Group* principal, conteniendo métricas aditivas como Cantidad Vendida e Inventario.
* **Jerarquías de Navegación (Drill-down):** Se crearon vías de navegación lógica para los usuarios finales. En `DimFecha` se configuró la jerarquía *Calendario* (Año -> Mes -> Día), y en `DimProducto` la jerarquía de catálogo (Categoría -> Subcategoría -> Nombre). Se establecieron relaciones de atributo rígidas para optimizar el motor de agregación y evitar cuellos de botella en memoria.

### 5. Validación de Calidad y Resultados Analíticos
Tras la ejecución del flujo ETL y el procesamiento del cubo, las pruebas de auditoría y calidad de datos (Data Quality) arrojaron resultados perfectos, certificando que no hubo pérdida de información.
* **Conteo de Registros (Integridad):** `DimProducto` (12), `DimCliente` (7), `DimCanal` (3), `DimFecha` (120), `FactVentasInventario` (1000).

Para comprobar la funcionalidad analítica del modelo estrella, se ejecutó una consulta de inteligencia de negocios cruzando ventas con la dimensión de productos para identificar la rentabilidad por categoría, obteniendo el siguiente resultado certificado:

| Categoría | Total Unidades Vendidas | Ingresos Totales (Q) | Ganancia Neta (Q) |
| :--- | :--- | :--- | :--- |
| **Abarrotes** | 3,584 | 53,989.60 | 11,882.40 |
| **Snacks** | 4,984 | 22,186.80 | 8,850.20 |
| **Lácteos** | 2,584 | 17,782.80 | 5,143.60 |
| **Bebidas** | 1,300 | 7,412.00 | 2,134.00 |

**Interpretación de Negocio:** El modelo revela que, si bien la categoría *Snacks* genera el mayor volumen de tráfico (4,984 unidades), la categoría *Abarrotes* es la más rentable financieramente, liderando tanto en ingresos brutos como en ganancia neta. Esta visibilidad gerencial es el producto directo de la consolidación exitosa del Data Warehouse.
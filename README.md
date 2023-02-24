# clei23_lib_pyspark

*clei23_lib_pyspark* es una biblioteca hecha en Python + PySpark con la finalidad de proporcionar herramientas que faciliten el procesamiento y análisis de los datos en un dataframe Spark.

Esta biblioteca es 100% compatible con la plataforma **Databricks** y su entorno de trabajo. 

## Requisitos técnicos
1) Tener instalado Python 3.6 o superior (recomendable la versión 3.11).
2) Java 8 o superior. 
3) Apache Spark.
4) PySpark.

##  Guía resumen de cada función
A continuación se explicará como utilizar cada función de la biblioteca.

Para esta, guía se usará este dataset [datasetPrueba](csv_md\used_cars_data.csv) como dataframe de test

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df_cars = spark.read.csv('csv/used_cars_data.csv' , header=True,inferSchema=True )

```

### [group_count](features/conteo.py)
#### [ejemplo práctico group_count](notebooks/testing_count.ipynb)
*Esta función recibe un dataframe de PySpark y una lista de columnas a las que se le hará un conteo,
se hará el cálculo de la suma y el avg de las columnas numericas.\
La función devuelve un dataframe con las columnas agregas de conteo, suma y avg de cada columna numerica.\
Parameters:\
dataframe (DataFrame): Dataframe de PySpark que se desea imputar. \
    group_cols (list): Lista de los nombres de las columnas para agrupar y contar\
    Returns:\
    DataFrame: Dataframe de PySpark con las nuevas columnas de conteo y cálculos matemáticos.*

**ejemplo:**
```python
listaColumnas = ['brand' , 'model']
df_conteo1 = group_count(df_cars , lista1)
```
### [imputacion](features/imputacion.py)
#### [ejemplo práctico imputacion](notebooks/testing_imputacion.ipynb)

*La función 'imputación' es una función que permite reemplazar los valores nulos en un dataframe dado por un valor determinado.\    
    Argumentos:\
    dataframe (pyspark.sql.dataframe.DataFrame): El dataframe del que se quieren reemplazar los valores nulos.\
    columna (str): La columna que se desea imputar.\
    Opción (str): La opción de imputación que se desea aplicar. Puede ser "media", "mediana", "moda", "similitud", "cero", o "desconocido".\
    auxiliar (str, opcional): El nombre de una columna auxiliar que se utilizará en caso de que la opción elegida sea "similitud". Por defecto, no se proporciona.\
    Retorno:\
    pyspark.sql.dataframe.DataFrame: Un nuevo dataframe con los valores nulos en la columna dada reemplazados por el valor especificado.*

Para este ejemplo se usará un dataframe con datos nulos:  [dataset prueba nulos](csv_md\cars_null.csv) 

```python
df_null = spark.read.csv('csv/cars_null.csv' , header=True,inferSchema=True )

```
**ejemplo:**
```python
df_limpio = imputacion(df_null , 'fuel' , 'moda' )
```
### [InferSchema](features/inferSchema.py)
#### [ejemplo práctico inferSchema](notebooks/testing_inferschema.ipynb)
*La función 'inferSchema' es una función que infiere y transforma los tipos de datos de cada columna a los que corresponda.\
    La función recorre cada columna tomando el primer tipo de dato de la columna para evaluarlo, dependiendo de esa evaluación
    el tipo de dato de la columna cambiará.\
    El patrón que sigue la función para identificar el tipo de dato es verificar caracteres que se obtenga de la muestra, debido a esto
    la función primero transforma todas las columnas a string.\
    Argumentos:\
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe al que se le desea hacer un cambio de tipo de datos de cada columna.\
    Retorno\
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe con tipo de datos de las columnas inferidas.*

**Ejemplo:** Supongamos que tenemos un dataframe al que queremos inferir todo el schema aun si ya tiene el schema inferido por spark.
```python
df_cars = spark.read.csv('csv/used_cars_data.csv' , header=True ,inferSchema=True)
```
Podemos utilizar la función inferSchema y este va a inferir los tipos de datos automáticamente.
```python
df_infer = inferSchema(df_cars)
```

### [seguimiento](features/seguimiento.py)
#### [ejemplo práctico seguimiento](notebooks/testing_seguimiento.ipynb)
*La función 'seguimiento' es una función que permite realizar un seguimiento a los valores de una columna mediante otra columna de tiempo.\
    Esta función realiza un seguimiento de los valores únicos de la columna seleccionada, calculando la variación neta y variación porcentual
    respecto a un periodo de tiempo dado.\
     Recibe un parámetro de tolerancia que sirve como referencia para indicar cuando una variación
    porcentual está fuera de los rangos esperados.\
    Argumentos:\
    df -- Dataframe a procesar.\
    tiempo -- Nombre de la columna en el dataframe que representa el tiempo.\
    seguimiento -- Nombre de la columna en el dataframe que representa el seguimiento.\
    Tolerancia -- Valor opcional de tolerancia para los cálculos. Valor por defecto: 50.\
    Retorno:\
    Dataframe con los resultados de los cálculos.\
    Lanza una excepción con un mensaje de error si ocurre un error durante el procesamiento de los datos.*

**Ejemplo:** Si queremos hacer seguimiento a los valores de la columna 'fuel' en relación con la columna de tiempo 'year' y que nos diga si el cambio es mayor a 75%.
```python
df_seguimiento = seguimiento(df_cars , 'year' , 'fuel', 75)
```

### [summary](features/summary.py)
#### [ejemplo práctico summary](notebooks/testing_summary.ipynb)
*Esta función recibe un dataframe de PySpark.\
 La función devuelve un dataframe con un resumen estadístico de todas las
    columnas de tipo numericas, este resumen toma como base a la función Summary()
    de pyspark y le agrega la moda y antimoda al dataframe.\
    En caso de que no haya 
    columnas tipo numéricas, la función no hará nada y retornará el mismo dataframe entregado.\
    Argumento:\
    dataframe (DataFrame): Dataframe de PySpark que se desea imputar.\
    Retorno:\
    DataFrame: Dataframe de PySpark con el resumen estadístico mejorado en caso de tener columnas de tipo numéricas.*
```python
df_sumary= summary(df_cars)
```

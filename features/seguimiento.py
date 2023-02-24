#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count, lag, lit, when , regexp_replace 
from validaciones import is_dataframe


def seguimiento(df, tiempo, seguimiento, tolerancia=50):
    """
    La función 'seguimiento' es una función que permite realizar un seguimiento a los valores de una columna mediante otra columna de tiempo.
    Esta función realiza un seguimiento de los valores únicos de la columna seleccionada, calculando la variación neta y variacion porcentual
    respecto a un periodo de tiempo dado. Recibe un parámetro de tolerancia que sirve como referencia para indicar cuando una variación
    porcentual está fuera de los rangos esperados.
    Argumentos:
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe a procesar.
    Tiempo (str): Nombre de la columna en el dataframe que representa el tiempo.
    Seguimiento (str): Nombre de la columna en el dataframe que representa el seguimiento.
    Tolerancia (int): Valor opcional de tolerancia para los cálculos. Valor por defecto: 50.
    Retorno:
    dataframe (pyspark.sql.dataframe.DataFrame) con los resultados de los cálculos.
    Lanza una excepción con un mensaje de error si ocurre un error durante el procesamiento de los datos.
    """

    # Verificar si es un dataframe
    is_dataframe(df)

    # Verificar el tipo de datos de tiempo
    data_type = df.schema[tiempo].dataType.simpleString()

    # Convertir tiempo a tipo de datos integer
    try:
        df = timeToInteger(data_type, df, tiempo)
    except Exception as e:
        print('Error al convertir el tipo de dato: ', e)
        # Remover espacios y caracteres especiales de seguimiento
    try:
        df = replaceExpChar(df, seguimiento)
    except Exception as e:
        print('error al intentar sacar los caracteres especiales o espacios: ', e)
    # Agrupar por tiempo y pivote seguimiento
    try:
        df = groupByTimePivot(df, seguimiento, tiempo)
    except Exception as e:
        print("Error al intentar agrupar por el pivot: ", e)

    # Rellenar valores nulos con 0
    try:
        for col in df.columns: df = df.fillna(0, col)
    except Exception as e:
        print("Error al rellenar los nulos con 0 ", e)
    # Calcular var_neta y var_porc
    try:
        df = var_neta_porc(df, tiempo, tolerancia)
        return df
    except Exception as e:
        print('Error al procesar los datos: ', e)


def var_neta_porc(df, tiempo, tolerancia):
    window = Window.partitionBy().orderBy(tiempo)
    for col in df.columns:
        df = df.withColumn('var_neta_' + col, df[col] - lag(df[col]).over(window))
        df = df.withColumn('var_porc_' + col,
                           when(lag(df[col]).over(window) == 0, 0)
                           .otherwise((df[col] - lag(df[col]).over(window)) / lag(df[col]).over(window) * 100))
        df = df.withColumn('fuera_de_tolerancia_' + col,
                           when(abs(df['var_porc_' + col]) > tolerancia, '*FUERA DE RANGO*')
                           .otherwise('dentro de rango'))
    df = df.withColumn("tolerancia", lit(tolerancia))
    return df


def groupByTimePivot(df, seguimiento, tiempo):
    df = df.groupBy(tiempo).pivot(seguimiento).agg(count("*"))
    df = df.sort(tiempo)
    return df


def replaceExpChar(df, seguimiento):
    df = df.withColumn(seguimiento, regexp_replace(df[seguimiento], " ", "_"))
    df = df.withColumn(seguimiento, regexp_replace(df[seguimiento], "[^a-zA-Z0-9]+", ""))
    return df


def timeToInteger(data_type, df, tiempo):
    if data_type == TimestampType().simpleString():
        df = df.withColumn(tiempo, df[tiempo].cast(TimestampType()).cast(IntegerType()))
    elif data_type == DateType().simpleString():
        df = df.withColumn(tiempo, df[tiempo].cast(DateType()).cast(IntegerType()))
    elif data_type == 'string':
        df = df.withColumn(tiempo, regexp_replace(df[tiempo], "-", ""))
        df = df.withColumn(tiempo, df[tiempo].cast(IntegerType()))
    return df

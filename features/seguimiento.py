from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count, lag, lit, when , regexp_replace , col
from validaciones import is_dataframe


def seguimiento (df , tiempo , seguimiento, tolerancia = 50):
    """
    La función 'seguimiento' es una función que permite realizar un seguimiento a los valores de una columna mediante otra columna de tiempo.
    Esta funcion realiza un seguimiento de los valores unicos de la columna seleccionada, calculando la variacion neta y variacion porcentual
    respecto a un periodo de tiempo dado. Recibe un parametro de tolerancia que sirve como referencia para indicar cuando una variacion
    porcentual esta fuera de los rangos esperados.

    Argumentos:
    df -- Dataframe a procesar.
    tiempo -- Nombre de la columna en el dataframe que representa el tiempo.
    seguimiento -- Nombre de la columna en el dataframe que representa el seguimiento.
    tolerancia -- Valor opcional de tolerancia para los cálculos. Valor por defecto: 50.

    Retorno:
    Dataframe con los resultados de los cálculos.

    Lanza una excepción con un mensaje de error si ocurre un error durante el procesamiento de los datos.
    """

    is_dataframe(df)
    data_type = df.schema[tiempo].dataType
    data_type = data_type.simpleString()
    
    try:  
        if data_type == TimestampType().simpleString():
            df = df.withColumn(tiempo ,df[tiempo].cast(DateType()))
            df = df.withColumn(tiempo ,df[tiempo].cast(StringType()))
            df = df.withColumn(tiempo, regexp_replace(df[tiempo], "-", ""))
            df = df.withColumn(tiempo ,df[tiempo].cast(IntegerType()))
        elif data_type == DateType().simpleString():
            df = df.withColumn(tiempo ,df[tiempo].cast(StringType()))
            df = df.withColumn(tiempo, regexp_replace(df[tiempo], "-", ""))
            df = df.withColumn(tiempo ,df[tiempo].cast(IntegerType()))
        elif data_type == 'string':   
            df = df.withColumn(tiempo, regexp_replace(df[tiempo], "-", ""))
            df = df.withColumn(tiempo ,df[tiempo].cast(IntegerType()))
    except Exception as e: 
        print('Error al convertir el tipo de dato: ' , e)
   
    try:
        df = df.withColumn(seguimiento, regexp_replace(df[seguimiento], " ", "_"))
        df = df.withColumn(seguimiento, regexp_replace(df[seguimiento], "[^a-zA-Z0-9]+", ""))
        df = df.groupBy(tiempo).pivot(seguimiento).agg(count("*"))
        df = df.sort(tiempo)
    except Exception as e:
        print('error al intentar sacar los caracteres especiales o espacios: ' , e)
    try:
                    
        df = df.withColumn(tiempo, df[tiempo].cast(IntegerType()))
        for col in df.columns : df = df.fillna(0 , col)
        for col in df.columns :
            df = df.withColumn('var_neta_'+col , df[col] - lag(df[col]).over(Window.partitionBy().orderBy(tiempo)))
        for col in df.columns:
            if col.startswith("var_neta_"):
                df = df.withColumn('var_porc_'+col, (df[col] / lag(df[col]).over(Window.partitionBy().orderBy(tiempo)))*100 - 100)
                df = df.withColumn("tolerancia", lit(tolerancia))
        for col in df.columns:
            if col.startswith("var_porc_"):
                df = df.withColumn('fuera_de_tolerancia_' + col, 
                                when(abs(df[col]) > tolerancia, '*FUERA DE RANGO*').otherwise('dentro de rango'))

        return df
    except Exception as e : 
        print('Error al procesar los datos: ' , e )   

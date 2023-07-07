#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################

from pyspark.sql.types import *
from pyspark.sql.functions import *
from validaciones import is_dataframe 


def imputacion(dataframe , columna , opcion , auxiliar = ''):
    """
    La función 'imputacion' es una función que permite reemplazar los valores nulos en un dataframe dado por un valor determinado. 
    
    Argumentos:
    dataframe (pyspark.sql.dataframe.DataFrame): El dataframe del que se quieren reemplazar los valores nulos.
    columna (str): La columna que se desea imputar.
    opcion (str): La opción de imputación que se desea aplicar. Puede ser "media", "mediana", "moda", "similitud", "cero", o "desconocido".
    auxiliar (str, optional): El nombre de una columna auxiliar que se utilizará en caso de que la opción elegida sea "similitud". 
                             Por defecto, no se proporciona.
    Retorno:
    pyspark.sql.dataframe.DataFrame: Un nuevo dataframe con los valores nulos en la columna dada reemplazados por el valor especificado.
    
    Ejemplos:
    >>> dataframe = spark.createDataFrame([(1, None), (2, 3.0), (3, 4.0)], ["id", "val"])
    >>> imputacion(dataframe, "val", "media")
    >>> # Returns:
    >>> # +---+-----+
    >>> # | id|  val|
    >>> # +---+-----+
    >>> # |  1| 3.25|
    >>> # |  2|  3.0|
    >>> # |  3|  4.0|
    >>> # +---+-----+
    
    """
    is_dataframe(dataframe)
    try:
        
        if dataframe.filter(isnull(columna)).count() > 0 : 
            tipos_numericos = [LongType().simpleString(), DoubleType().simpleString(), 
                IntegerType().simpleString() , ShortType().simpleString() ,
                FloatType().simpleString() , DecimalType().simpleString()]
            data_type = dataframe.schema[columna].dataType
            data_type = data_type.simpleString()
            
            if data_type in tipos_numericos:
                if opcion == 'mediana':
                    print('opcion de mediana')
                    mean_val = dataframe.select(mean(columna)).first()[0]
                    dataframe = dataframe.fillna(mean_val, [columna])
                    return dataframe
                elif opcion == 'media':
                    print('opcion de media')
                    median_val = dataframe.select(percentile_approx(columna, 0.5).alias("median")).first()[0]
                    dataframe = dataframe.fillna(median_val, [columna])
                    return dataframe
                elif opcion == 'moda':
                    print('opcion de moda')
                    mode_val = dataframe.groupBy(columna).count().sort('count', ascending=False).first()[0]
                    dataframe = dataframe.fillna(mode_val, [columna])
                    return dataframe
                elif opcion == 'similitud':
                    df_fill = similitud(dataframe , columna , auxiliar)
                    return df_fill
                elif opcion == 'cero':
                    dataframe = dataframe.fillna(0, [columna])
                    return dataframe
                            
            elif data_type == StringType().simpleString():
                lista_nulls = ['null' , '-' , '' , ' ', 'n' , '\\n' ]        
                if opcion == 'moda': 
                    print('opcion de moda')
                    mode_val = dataframe.filter(dataframe[columna].isNotNull()).groupBy(columna).count().sort('count', ascending=False).first()[0]
                    for i, row in enumerate(dataframe.toLocalIterator()):
                        if row[columna] in lista_nulls:
                            dataframe = dataframe.withColumn(columna, when(col(columna).isin(lista_nulls), mode_val).otherwise(col(columna)))  
                    return dataframe 
                elif opcion == 'cero': 
                    print('opcion de cero')
                    for i, row in enumerate(dataframe.toLocalIterator()):
                        if row[columna] in lista_nulls:
                            dataframe = dataframe.withColumn(columna, when(col(columna).isin(lista_nulls), '0').otherwise(col(columna)))  
                    return dataframe
                elif opcion == 'desconocido':
                    print('opcion de desconocido')
                    for i, row in enumerate(dataframe.toLocalIterator()):
                        if row[columna] in lista_nulls:
                            dataframe = dataframe.withColumn(columna, when(col(columna).isin(lista_nulls), 'desconocido').otherwise(col(columna)))  
                    return dataframe
        else: 
            print('No existen valores nulos en la columna indicada')
            return dataframe
    except Exception as e : 
        print('Error: ' , e)

def similitud(dataframe, columna_imputacion, columna_auxiliar):
    """
    Esta función recibe un dataframe de PySpark, una columna a ser imputada y una columna auxiliar. 
    La función devuelve el dataframe con la columna de imputación llenada con el promedio de los valores 
    de la columna de imputación agrupados por la columna auxiliar. Si el valor de la columna de imputación
    no es nulo, se mantiene el mismo valor.
    Parameters:
    dataframe (DataFrame): Dataframe de PySpark que se desea imputar
    columna_imputacion (string): Nombre de la columna que se desea imputar
    columna_auxiliar (string): Nombre de la columna auxiliar para calcular el promedio
    Returns:
    DataFrame: Dataframe de PySpark con la columna de imputación llenada con el promedio de los valores 
               de la columna de imputación agrupados por la columna auxiliar.
    """
    try:
        avg_vals = dataframe.filter(dataframe[columna_imputacion].isNotNull()).groupBy(columna_auxiliar).agg(avg(columna_imputacion))
        df_fill = dataframe.join(avg_vals, columna_auxiliar, 'left') \
                           .withColumn(columna_imputacion, when(dataframe[columna_imputacion].isNull(), col('avg(' + columna_imputacion + ')')).otherwise(dataframe[columna_imputacion])) \
                           .drop('avg(' + columna_imputacion + ')')
        return df_fill
    except Exception as e:
        print("Error: ", e)
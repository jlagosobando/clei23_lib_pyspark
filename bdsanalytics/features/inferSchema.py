#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################


from pyspark.sql.functions import col , regexp_replace
from pyspark.sql.types import *
from validaciones import is_dataframe 
import re 
def inferSchema(dataframe):
    """
    La función 'inferSchema' es una función que infiere y transforma los tipos de datos de cada columna a los que corresponda.
    La función recorre cada columna tomando el primer tipo de dato de la columna para evaluarlo, dependiendo de esa evaluación
    el tipo de dato de la columna cambiará.
    El patrón que sigue la función para identificar el tipo de dato es verificar caracteres que se obtenga de la muestra, debido a esto
    la función primero transforma todas las columnas a string.

    Argumentos:
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe al que se le desea hacer un cambio de tipo de datos de cada columna.

     Retorno
    dataframe (pyspark.sql.dataframe.DataFrame): Dataframe con tipo de datos de las columnas inferidas.
    """
    is_dataframe(dataframe) 
    try:
        dataframe = dataframe.select([col(c).cast("string") for c in dataframe.columns])
        for columna in dataframe.columns: 
            first_value = dataframe.select(col(columna)).first()[0]
            if contains_letter(first_value):
                dataframe = dataframe.withColumn(columna , col(columna).cast(StringType())) 
                print(f'{columna} | string -> string')
            elif columna == "Date":
                contador = first_value.count(".")
                if '-' in first_value:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))  
                    print(f'{columna} | string -> date')     
                elif contador == 2: 
                    dataframe = dataframe.withColumn(columna, regexp_replace(dataframe[columna], "[^a-zA-Z0-9]+", ""))
                    dataframe = dataframe.withColumn(columna , col(columna).cast((IntegerType()))) 
                    print(f'{columna} | string -> integer')
            elif '.' in first_value:
                contador = first_value.count(".")
                if contador == 2: 
                    dataframe = dataframe.withColumn(columna, regexp_replace(dataframe[columna], "[^a-zA-Z0-9]+", ""))
                    dataframe = dataframe.withColumn(columna , col(columna).cast((IntegerType()))) 
                    print(f'{columna} | string -> date')
                elif contador == 1:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(DoubleType()))
                    print(f'{columna} | string -> double') 
                else:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
                    print(f'{columna} | string -> string')
            elif "-"  in first_value:
                contador = first_value.count("-")
                if contador == 2:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))  
                    print(f'{columna} | string -> date')
                else: 
                    dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
                    print(f'{columna} | string -> string')
            elif "/" in first_value:
                contador = first_value.count("/")
                if contador == 2:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))
                    print(f'{columna} | string -> date')
                else: 
                    dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
                    print(f'{columna} | string -> string')

            elif "True" in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
                print(f'{columna} | string -> bool')
            elif "False" in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
                print(f'{columna} | string -> bool')
            elif first_value.isnumeric():
                dataframe = dataframe.withColumn(columna , col(columna).cast(LongType()))
                try:
                    dataframe = dataframe.withColumn(columna , col(columna).cast((IntegerType())))
                    print(f'{columna} | string -> integer')
                except:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(LongType()))
                    print(f'{columna} | string -> long')
                    pass
            else:
                dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
                print(f'{columna} | string -> string')
        return dataframe
    except Exception as e :
        print('Ha ocurrido un erro al momento de inferir el schema: ' , e )

def contains_letter(string):
    return bool(re.search("[a-zA-Z]", string))

#######################################                                  
# fecha: 14-01-2023                   #      
# ultima modificacion: 14-02-2023     #  
# Gabriel Aillapan                    #                
#######################################


# IMPORTANTE : agregar importanciones si hace falta
from pyspark.sql.types import *
from pyspark.sql.functions import isnull
from pyspark.sql.dataframe import DataFrame
def empty_df_rows(dataframe):
    """
    La función empty_df_rows toma como argumento un objeto de tipo DataFrame de pyspark y verifica si el dataframe tiene alguna fila.
    Si el dataframe tiene alguna fila devuelve False, de lo contrario devuelve True.
    Si el argumento no es un DataFrame o si ocurre alguna excepción durante la ejecución de la función, se imprimirá un mensaje de error en la consola.
    """
    try:
        is_dataframe(dataframe)
        if dataframe.count() == 0:  return True  
        return False 
    except Exception as e : 
        print("An error occurred: ", e)
def empty_df_columns(dataframe): 
    """
    La función "empty_df_columns" recibe como parámetro un dataframe de pyspark.
    Utiliza una función auxiliar "is_dataframe" para verificar si el objeto pasado como parámetro es realmente un dataframe.
    La función trata de obtener la cantidad de columnas del dataframe. 
    Si es igual a cero, devuelve True, indicando que el dataframe no tiene columnas.
    Si no es igual a cero, devuelve False.
    Si ocurre algún error, se imprime un mensaje de error
    """
    try:
        is_dataframe(dataframe)
        if len(dataframe.columns) == 0: return True
        return False
    except Exception as e :
        print("An error occurred: ", e)
def df_has_numtype(dataframe):
    """
    La función df_has_numtype(dataframe) tiene como objetivo determinar si al menos una de las columnas de un DataFrame dado contiene un tipo de datos numérico.
    La función toma como entrada un DataFrame y devuelve un valor booleano (True si al menos una columna es numérica, False en caso contrario).
    La función comienza verificando si la entrada es realmente un DataFrame utilizando la función is_dataframe().
    A continuación, se crea una lista de tipos de datos numéricos válidos.
    Luego, se recorren los tipos de datos de cada columna del DataFrame y si al menos uno de ellos está en la lista de tipos de datos numéricos válidos, la función devuelve True.
    Si ninguno de los tipos de datos de las columnas del DataFrame es numérico, la función devuelve False. 
    En caso de que ocurra algún error durante la ejecución de la función, se imprimirá un mensaje de error.
    """

    try:
        is_dataframe(dataframe)
        tipos_numericos = [LongType().simpleString(), DoubleType().simpleString(), 
        IntegerType().simpleString() , ShortType().simpleString() ,
        FloatType().simpleString() , DecimalType().simpleString()]
        for _ , dtype in dataframe.dtypes: 
            if dtype in tipos_numericos:  return True 
        return False
    except Exception as e:
        print("An error occurred: ", e)
def df_has_null(dataframe):
    """
    La función df_has_null recibe un dataframe de pyspark como parámetro y verifica si alguna de las columnas del dataframe contiene valores nulos.
    Utiliza la función isnull para detectar los valores nulos en cada columna. 
    Si encuentra alguna columna con valores nulos, devuelve una tupla con el valor booleano True y el nombre de la columna con valores nulos.
    En caso contrario, devuelve una tupla con el valor booleano False y None.
    Si se produce algún error en el proceso, se imprime un mensaje de error.
    """
    try:    
        is_dataframe(dataframe)
        for col in dataframe.columns:
            if dataframe.filter(isnull(col)).count() > 0 : return (True , col)
        return (False , None)
    except Exception as e: 
        print("An error occurred: ", e)
def is_dataframe(dataframe):
    """
    La función is_dataframe(dataframe) recibe un objeto dataframe y verifica si ese objeto es del tipo DataFrame de pyspark.
    Si el objeto no es un DataFrame, la función lanza una excepción TypeError indicando que se esperaba un objeto DataFrame pero se recibió un objeto de otro tipo.
    Si no se encuentra ningún error, la función no devuelve ningún valor.
    La función también tiene un bloque try-except que captura cualquier excepción que pueda ocurrir y muestra un mensaje de error.  
    """
    try:
        type_df = type(dataframe)
        if not isinstance(dataframe , DataFrame):
            raise TypeError(f"Expected a DataFrame, got {type_df}")
    except Exception as e: 
        print("An error occurred: ", e)
def is_all_string(dataframe):
    """
    La función is_all_string recibe como parámetro un dataframe de pyspark y verifica si todas las columnas del dataframe son de tipo string.
    La función utiliza el método dtypes del dataframe para obtener una lista de tuplas con las columnas y sus tipos de datos correspondientes.
    La función recorre esta lista y verifica si el tipo de dato de cada columna es de tipo string, si es así, incrementa el contador 'num' en 1.
    Si el contador 'num' es igual al número total de columnas del dataframe, se devuelve una tupla con el valor booleano 'True' y 'None' indicando que todas las columnas son de tipo string.
    En caso contrario, se devuelve una tupla con el valor booleano 'False' y una lista con las columnas que no son de tipo string.
    En caso de que ocurra algún error, se imprime un mensaje con la excepción.
    """
    try:
        is_dataframe(dataframe)
        num = 0
        num_col_df = len(dataframe.columns)
        columns = []
        string_type = StringType().simpleString()
        for col , dtype in dataframe.dtypes:
            if dtype == string_type:
                num = num + 1
            if dtype != string_type: columns.append(col)
        if num == num_col_df: return (True , None)  
        return (False , columns) 
    except Exception as e:
        print("An error ocurred: " , e)
def all_string_columns(dataframe):
    """
    all_string_columns(dataframe) es una función que recibe como parámetro un DataFrame de Spark y devuelve un valor booleano indicando si todas las columnas del DataFrame son del tipo String.
    La función comienza verificando que el parámetro recibido sea un DataFrame utilizando la función is_dataframe(dataframe).
    Si el parámetro no es un DataFrame, se lanzará una excepción de tipo TypeError.
    A continuación, se itera sobre las columnas del DataFrame utilizando el método dtypes para obtener el tipo de datos de cada columna.
    Si alguna columna no es de tipo String, se devuelve False. Si todas las columnas son de tipo String, se devuelve True.
    En caso de que ocurra algún error durante la ejecución de la función, se imprimirá un mensaje de error en pantalla.
    """
    try:
        is_dataframe(dataframe)
        for _, dtype in dataframe.dtypes:
            if dtype != StringType().simpleString():
                return False
        return True
    except Exception as e:
        print("An error occurred: ", e)
def df_analyzer(dataframe): 
    """
    df_analyzer es una funcion que recibe como parametroun Dataframe de Spark y utiliza las validaciones credas para dar a conocer posibles inconveniente que puede tener un dataframe. 
    """
    try:
        is_dataframe(dataframe)
        result_bool , result_column = df_has_null(dataframe)
        result_strings , columns_not_strings = is_all_string(dataframe)
        check = "\u2714"
        x = "\u274C"
        resultados = []
        if empty_df_rows(dataframe):
            resultados.append(f" rows: {x}   Dataframe has not rows" )
        else:
            resultados.append(f"rows : {check}")
        if empty_df_columns(dataframe):
            resultados.append(f" colums: {x}  Dataframe has empty colums")
        else:
            resultados.append(f"colums: {check}")
        
        if result_strings:
            resultados.append(f"colums strings : {x} Dataframe has only string type columns")
        else: 
            resultados.append(f"Dataframe has at least one column that is not a string type {check} \n  Columns not string -> {columns_not_strings}")

        if df_has_numtype(dataframe):
            resultados.append(f"number types {check} ")
        
        else:
            resultados.append(f"number types: {x}  Dataframe has not number types")
        if result_bool: 
            resultados.append(f"null data: {x}  Dataframe has null data \n      -> Column with null data : {result_column}")            
        else:
            resultados.append(f"not null :{check}")
        for resultado in resultados:
            print(resultado)
    except Exception as e:
        print("An error occurred: ", e)


from utils import get_credentials, process_json, snowflake_login, create_email
from config import configuration
from datetime import date, timedelta, datetime

import time

def type_of_report(fecha: str, tipo: str) -> None:
    
    #Busco que funcion ejecutar en base al parametro que me llega:
    functions = configuration[tipo]['FUNCTION']
    eval(functions + "('" + fecha + "', '" + tipo + "')")

def pagnifique_report(fecha: str, tipo: int) -> None:

    #Defino fechas para saber cuando ejecuto
    ini_date = datetime.strptime(fecha, '%Y-%m-%d').date()
    end_date = ini_date - timedelta(days=6)

    #Obtengo las credenciales de email
    email_credentials = get_credentials('Correo')

    #Obtengo datos de quien recibe y asunto
    information = configuration[tipo]
    
    #Obtengo curso y login de Snowflake
    cs = snowflake_login()

    #Proceso las query que tengo generadas en json
    queries = process_json()

    #Obtengo el texto de mi query
    with open(queries['PAGNIFIQUE_INFORME'], 'r') as file:
        command = file.read().format(end_date = end_date, 
                                    ini_date = ini_date,
                                    end_date_1 = end_date, 
                                    ini_date_1 = ini_date)

    #Ejecuto query
    cs.execute(command)
    
    #Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cs.fetch_pandas_all()

    #Verifico que el DF no este vacio para luego descargarlo.
    if len(df) > 0:
        
        #Descargo el archivo en la ubicacion que le dejo en config
        df.to_excel(f'{information["FOLDER"]}/Pagnifique_Semana_{ini_date} al {end_date}.xlsx', index = False)

    else:
        #Emito error
        print("No se pudo descargar el informe")

    #Hago un sleep de 15 por si demora en crearse el Excel y el mail sale rápido
    time.sleep(10)

    #Genero el email para enviar
    email = create_email(sender_address= email_credentials['USER'], 
                        sender_pass= email_credentials['PASSCODE'],
                        receiver_address= information['RECEIVER'],
                        message_type= information['NAME'], 
                        subject= information['SUBJECT'])

    if email:
        print("Mail enviado correctamente")
    else:
        print("Algo falló")

def pagnifique_monthly_report(fecha: str, tipo: int) -> None:

    #Defino fechas para saber cuando ejecuto
    current_date = datetime.strptime(fecha, '%Y-%m-%d').date()

    #Obtengo las credenciales de email
    email_credentials = get_credentials('Correo')

    #Obtengo datos de quien recibe y asunto
    information = configuration[tipo]
    
    #Obtengo curso y login de Snowflake
    cs = snowflake_login()

    #Proceso las query que tengo generadas en json
    queries = process_json()

    #Obtengo el texto de mi query
    with open(queries['PAGNIFIQUE_INFORME_MENSUAL'], 'r') as file:
        command = file.read()

    #Ejecuto query
    cs.execute(command)
    
    #Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cs.fetch_pandas_all()

    #Verifico que el DF no este vacio para luego descargarlo.
    if len(df) > 0:
        
        #Descargo el archivo en la ubicacion que le dejo en config
        df.to_excel(f'{information["FOLDER"]}/Pagnifique_Reporte_Mensual_Mes_Anterior_{current_date}.xlsx', index = False)

    else:
        #Emito error
        print("No se pudo descargar el informe")

    #Hago un sleep de 15 por si demora en crearse el Excel y el mail sale rápido
    time.sleep(10)

    #Genero el email para enviar
    email = create_email(sender_address= email_credentials['USER'], 
                        sender_pass= email_credentials['PASSCODE'],
                        receiver_address= information['RECEIVER'],
                        message_type= information['NAME'], 
                        subject= information['SUBJECT'])

    if email:
        print("Mail enviado correctamente")
    else:
        print("Algo falló")

def pagnifique_results(fecha: str, tipo: str) -> None:

    actual_date = datetime.strptime(fecha, '%Y-%m-%d').date()

    #Obtengo las credenciales de email
    email_credentials = get_credentials('Correo')

    #Obtengo datos de quien recibe y asunto
    information = configuration[tipo]
    
    #Obtengo curso y login de Snowflake
    cs = snowflake_login()

    #Proceso las query que tengo generadas en json
    queries = process_json()

    #Obtengo el texto de mi query
    with open(queries['PAGNIFIQUE_RESULTADOS'], 'r') as file:
        command = file.read()

    #Ejecuto query
    cs.execute(command)
    
    #Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cs.fetch_pandas_all()

    #Verifico que el DF no este vacio para luego descargarlo.
    if len(df) > 0:
        
        #Descargo el archivo en la ubicacion que le dejo en config
        df.to_excel(f'{information["FOLDER"]}/Resultados Promos al {actual_date}.xlsx', index = False)

    else:
        #Emito error
        print("No se pudo descargar el informe")

    #Hago un sleep de 15 por si demora en crearse el Excel y el mail sale rápido
    time.sleep(10)

    #Genero el email para enviar
    email = create_email(sender_address= email_credentials['USER'], 
                        sender_pass= email_credentials['PASSCODE'],
                        receiver_address= information['RECEIVER'],
                        message_type= information['NAME'], 
                        subject= information['SUBJECT'])

    if email:
        print("Mail enviado correctamente")
    else:
        print("Algo falló")

def compare_points_clients(fecha: str, tipo: str) -> None:
    
    actual_date = datetime.strptime(fecha, '%Y-%m-%d').date()

    #Obtengo las credenciales de email
    email_credentials = get_credentials('Correo')

    #Obtengo datos de quien recibe y asunto
    information = configuration[tipo]
    
    #Obtengo curso y login de Snowflake
    cs = snowflake_login()

    #Proceso las query que tengo generadas en json
    queries = process_json()

    #Obtengo el texto de mi query
    with open(queries['COMPARACION_INFORME'], 'r') as file:
        command = file.read()

    #Ejecuto query
    cs.execute(command)
    
    #Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cs.fetch_pandas_all()

    #Verifico que el DF no este vacio para luego descargarlo.
    if len(df) > 0:
        
        #Descargo el archivo en la ubicacion que le dejo en config
        df.to_excel(f'{information["FOLDER"]}/Comparacion anual al {actual_date}.xlsx', index = False)

    else:
        #Emito error
        print("No se pudo descargar el informe")

    #Hago un sleep de 15 por si demora en crearse el Excel y el mail sale rápido
    time.sleep(10)

    #Genero el email para enviar
    email = create_email(sender_address= email_credentials['USER'], 
                        sender_pass= email_credentials['PASSCODE'],
                        receiver_address= information['RECEIVER'],
                        message_type= information['NAME'], 
                        subject= information['SUBJECT'])

    if email:
        print("Mail enviado correctamente")
    else:
        print("Algo falló")

def clientes_evolution(fecha: str, tipo: str) -> None:
    
    actual_date = datetime.strptime(fecha, '%Y-%m-%d').date()

    #Obtengo las credenciales de email
    email_credentials = get_credentials('Correo')

    #Obtengo datos de quien recibe y asunto
    information = configuration[tipo]
    
    #Obtengo curso y login de Snowflake
    cs = snowflake_login()

    #Proceso las query que tengo generadas en json
    queries = process_json()

    #Obtengo el texto de mi query
    with open(queries['EVOLUCION_SOCIOS'], 'r') as file:
        command = file.read()

    #Ejecuto query
    cs.execute(command)
    
    #Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cs.fetch_pandas_all()

    #Verifico que el DF no este vacio para luego descargarlo.
    if len(df) > 0:
        
        #Descargo el archivo en la ubicacion que le dejo en config
        df.to_excel(f'{information["FOLDER"]}/Evolucion socios al {actual_date}.xlsx', index = False)

    else:
        #Emito error
        print("No se pudo descargar el informe")

    #Hago un sleep de 15 por si demora en crearse el Excel y el mail sale rápido
    time.sleep(10)

    #Genero el email para enviar
    email = create_email(sender_address= email_credentials['USER'], 
                        sender_pass= email_credentials['PASSCODE'],
                        receiver_address= information['RECEIVER'],
                        message_type= information['NAME'], 
                        subject= information['SUBJECT'])

    if email:
        print("Mail enviado correctamente")
    else:
        print("Algo falló")
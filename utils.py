import json
import snowflake.connector
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from datetime import date
from config import configuration

def get_credentials(type: str) -> dict:

    f = open('credentials.json',)
    credentials = json.load(f)[type]

    return credentials

def snowflake_login()-> snowflake.connector.connection.SnowflakeConnection:

    credentials = get_credentials('Snowflake')

    USER, PASS, ACCOUNT = credentials['USER'], credentials['PASS'], credentials['ACCOUNT']

    ctx = snowflake.connector.connect(
            user=USER,
            password=PASS,
            account=ACCOUNT,
            database="SANDBOX_PLUS",
            schema="DWH"
            )
    
    cs = ctx.cursor()

    return cs

def process_json():

    # Lee y devuelve sql_informes.json
    f = open('sql_informes.json',)
    data = json.load(f)

    return data

def search_message(message: str) -> str:

    if message == "PAGNIFIQUE_PENETRACION":

        content= """
            Hola, envío informe de % de penetracion de los productos pagnifique de la última semana, los archivos quedan en la siguiente ubicación:
            
            https://drive.google.com/drive/folders/1Ys4gGePqMJvS_7qYEwbK1iDLZYsGneWA?usp=sharing
            
            Saludos.
        """

    elif message == "PAGNIFIQUE_RESULTADOS_PROMO":

        content = """
            Hola, envío informe de promociones, el archivo queda en el siguiente repositorio:
            
            https://drive.google.com/drive/folders/1dPIaAcAZRuJ3rS1-LZz0LGnxHhwTBFkw?usp=sharing
            
            Saludos.
        """

    elif message == "PAGNIFIQUE_PENETRACION_MENSUAL":

        content= """
            Hola, envío informe de % de penetracion de los productos pagnifique del último mes, los archivos quedan en la siguiente ubicación:
            
            https://drive.google.com/drive/folders/1Ys4gGePqMJvS_7qYEwbK1iDLZYsGneWA?usp=sharing
            
            Saludos.
        """

    elif message == "COMPARACION_PUNTOS_CLIENTES":

        content= """
            Hola, envío comparación de puntos de clientes e información adicional, los archivos quedan en la siguiente ubicación:
            
            https://drive.google.com/drive/folders/1s2apC7R7u9SciMIko2IS6Hv8rrseqn9T?usp=sharing
            
            Saludos.
        """

    elif message == "EVOLUCION_SOCIOS":

        content= """
            Hola, envío evolución de clientes, los archivos quedan en la siguiente ubicación:
            
            https://drive.google.com/drive/folders/1j7GjPaWyxLiL2SYX6LOJ6tPnSrElmoZ0?usp=sharing
            
            Saludos.
        """
    
    else:
        
        content = """
            Este es un mensaje de prueba
        """    
    
    return content

def create_email(sender_address: str, receiver_address: str, sender_pass: str , message_type: str, subject: str) -> bool:
    
    #Defino mensaje a incluir
    content = search_message(message_type)

    #Instancio mensaje a enviar:
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = subject

    #Armo cuerpo de mail
    message.attach(MIMEText(content, 'plain'))

    #Llamo funcion para enviar mail
    envio = send_email(message= message, sender_address= sender_address, sender_pass= sender_pass)

    if envio:
        return True
    else:
        return False

def send_email(message: str, sender_address: str, sender_pass: str) -> bool:

    #sesion SMTP
    session = smtplib.SMTP('smtp.gmail.com', 587)
    #Autorizacion seguridad
    session.starttls()
    #Login con credenciales
    session.login(sender_address, sender_pass)
    text = message.as_string()
    #Envio mail
    session.sendmail(sender_address, message["To"].split(","), text)
    #Cierro sesion
    session.quit()
    
    return True
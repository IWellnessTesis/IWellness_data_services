import pika
import pymongo
import json

# Configuración de MongoDB
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "DB-I-Wellness"
COLLECTION_NAME = "I-Wellness"

# Conectar a MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Insertar servicio en MongoDB
def guardar_en_db(data):
    try:
        collection.insert_one(data)
        print(f"Servicio guardado en MongoDB: {data['nombre']}")
    except Exception as e:
        print(f"Error guardando en MongoDB: {e}")

# Conexión con RabbitMQ
def callback(ch, method, properties, body):
    try:
        mensaje = json.loads(body.decode('utf-8'))
        print(f"Mensaje recibido: {mensaje}")
        guardar_en_db(mensaje)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error procesando mensaje: {e}")

def consumir_mensajes():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='my_queue', durable=True)
    channel.basic_consume(queue='my_queue', on_message_callback=callback)

    print(" [*] Esperando mensajes. Para salir presiona CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    consumir_mensajes()

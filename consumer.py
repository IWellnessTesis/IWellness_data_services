import pika
import mysql.connector
import json

# Configuración de MySQL
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'db_iwellness'
}

# Mapeo de colas a tablas
QUEUE_TABLE_MAPPING = {
    'queue_preference': 'Preferencias',
    'queue_servicioxpreferencia': 'ServicioXPreferencia',
    'queue_turistaxpreferencia': 'TuristaXPreferencias',
    'queue_services': 'Servicio',
    'queue_turista': 'Turista',
    'queue_proveedor': 'Proveedor',
    'queue_users': 'Usuarios'
}

# Conexión a MySQL
def get_db_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

# 🔍 Elimina campos con valor None
def limpiar_documento(doc):
    return {k: v for k, v in doc.items() if v is not None}

# 🔁 Intenta deserializar varias veces hasta obtener un dict
def deserializar_recursivo(payload):
    intentos = 0
    while isinstance(payload, str) and intentos < 5:
        try:
            payload = json.loads(payload)
            intentos += 1
        except json.JSONDecodeError:
            break
    return payload

# 🔄 Transforma campos especiales antes de guardar
def transformar_mensaje(data, queue_name):
    # Procesar campos anidados según la cola
    if queue_name in ['queue_servicioxpreferencia', 'queue_turistaxpreferencia']:
        preferencia = data.get("preferencia")
        if isinstance(preferencia, dict):
            # Extraer _idPreferencias del objeto anidado
            if "_idPreferencias" in preferencia:
                data["_idPreferencias"] = preferencia["_idPreferencias"]
            # Eliminar el objeto anidado original
            data.pop("preferencia", None)
    
     # 🛠️ Corregir nombre del campo para TuristaXPreferencias
    if queue_name == 'queue_turistaxpreferencia':
        if "_idTuristaXPreferencia" in data:
            data["_idTuristaXPreferencias"] = data.pop("_idTuristaXPreferencia")
    
 # 👇 Nueva lógica para extraer el ID del usuario
    if queue_name == 'queue_proveedor':
        if isinstance(data.get("usuarios"), dict):
            usuario_id = data["usuarios"].get("id")
            if usuario_id is not None:
                data["usuarios_id"] = usuario_id
            data.pop("usuarios", None)

    return data

# ✅ Guardar documento en MySQL
def guardar_en_db(mensaje, queue_name):
    try:
        if isinstance(mensaje, bytes):
            mensaje = mensaje.decode("utf-8")

        print(f"🔍 Mensaje como string inicial: {mensaje}")

        data = deserializar_recursivo(mensaje)

        if not isinstance(data, dict):
            raise ValueError(f"❌ Mensaje no es un dict después de intentar deserializar: {type(data)}")

        # Transformar datos según la cola
        data = transformar_mensaje(data, queue_name)

        # Limpiar campos nulos
        data = limpiar_documento(data)

        # Conectar a MySQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Preparar la consulta SQL
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO {QUEUE_TABLE_MAPPING[queue_name]} ({columns}) VALUES ({values})"

        # Ejecutar la consulta
        cursor.execute(sql, list(data.values()))
        conn.commit()

        print(f"💾 Guardando en MySQL - Tabla {QUEUE_TABLE_MAPPING[queue_name]}: {data}")
        print(f"✅ Guardado exitosamente en MySQL")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error guardando en MySQL: {e}")
        print(f"❌ Datos que causaron el error: {data}")


# 🎧 Generador de callbacks por cola
def crear_callback(nombre_cola):
    def callback(ch, method, properties, body):
        try:
            print(f"📩 Mensaje recibido de la cola: {nombre_cola}")
            print(f"📦 Properties: {properties}")
            print(f"📝 Body: {body}")
            guardar_en_db(body, nombre_cola)
        except Exception as e:
            print(f"❌ Error en callback para la cola {nombre_cola}: {e}")
    return callback

# 🚀 Escuchar mensajes desde RabbitMQ
def consumir_mensajes():
    try:
        credentials = pika.PlainCredentials('user', 'user')
        parameters = pika.ConnectionParameters(
            host='localhost',
            port=5672,
            credentials=credentials,
            heartbeat=60,
            connection_attempts=5,
            retry_delay=5
        )

        print("🔄 Intentando conectar con RabbitMQ...")
        connection = pika.BlockingConnection(parameters)
        print("✅ Conexión establecida con RabbitMQ")

        channel = connection.channel()
        print("✅ Canal creado exitosamente")

        for queue_name in QUEUE_TABLE_MAPPING.keys():
            try:
                print(f"\n📝 Procesando cola: {queue_name}")
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=crear_callback(queue_name),
                    auto_ack=True
                )
                print(f"✅ Consumo configurado para {queue_name}")
            except Exception as e:
                print(f"❌ Error al configurar la cola {queue_name}: {e}")
                continue

        print("\n✅ Todas las colas configuradas exitosamente")
        print(f" [*] Esperando mensajes de las colas: {list(QUEUE_TABLE_MAPPING.keys())}")
        print("Para salir presiona CTRL+C")

        channel.start_consuming()

    except Exception as e:
        print(f"🚫 Error inesperado: {e}")
        print("Detalles del error:", str(e))

if __name__ == "__main__":
    consumir_mensajes()

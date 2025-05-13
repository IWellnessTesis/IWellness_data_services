# 📊 IWellness_data_services

Microservicio del ecosistema **IWellness** encargado de la **analítica de datos**.

Este componente recibe datos desde la cola de mensajería (RabbitMQ), los procesa según reglas definidas y genera información estructurada que puede ser consumida por el frontend o almacenada en la base de datos del sistema.

---

## ⚙️ Funcionalidades principales

- Escucha mensajes desde colas específicas (RabbitMQ).
- Realiza limpieza, transformación y análisis de los datos.
- Genera estructuras listas para visualización o persistencia.
- Envia la información procesada a la base de datos `IWellness-DB`.
- Envia la informaciónd de la base de datos al front

---

## 🚀 ¿Cómo ejecutar?

> 💡 Requisitos:
> - Docker y Docker Compose instalados
> - Acceso a las colas de RabbitMQ (`Queue-Rabbit`)
> - Base de datos en funcionamiento (`IWellness-DB`)

### 1. Clona el repositorio

```bash
git clone https://github.com/tu-usuario/IWellness_data_services.git
cd IWellness_data_services
```
### 2. Instala las dependencias necesarias
#### Para consumer.py:
```bash
pip install pika
pip install mysql.connector
```
#### Para data_analisys.py
```bash
pip install flask
```
### 3. Ejecutar micros
Se puede hacer de dos formas:
1. Ejecutar el programa normal desde el IDE, crear una terminal para cada .py
2. Abrir dos terminales y ejecutar el siguiente comando
```bash
python consumer.py
python data_analisys.py
```
Nota: Recuerda verificar que estas dentro de la carpeta del repositorio


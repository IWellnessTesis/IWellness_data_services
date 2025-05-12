from flask import Flask, jsonify
import mysql.connector
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:4200"}})
# Conexión a la base de datos
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='db_iwellness'
    )

@app.route('/api/servicios', methods=['GET'])
def obtener_servicios():
    conn = get_db_connection()
    query = """
        SELECT serviceName, coordenadaX, coordenadaY, estado, 
               CASE WHEN estado = 1 THEN 'Activo' ELSE 'Inactivo' END as estado_texto
        FROM Service_Location_Info
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    
    # Devolver los resultados como JSON
    return jsonify(resultados)

@app.route('/api/preferencias-usuario', methods=['GET'])
def obtener_preferencias_usuario():
    conn = get_db_connection()
    query = """
        SELECT genero, estadoCivil, intereses,COUNT(*) as total
        FROM Service_Search_By_UserStatus
        GROUP BY genero, estadoCivil, intereses
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)


@app.route('/api/intereses-usuario', methods=['GET'])
def obtener_intereses_usuario():
    conn = get_db_connection()
    query = """
        SELECT pais, intereses, COUNT(*) as total
        FROM User_Interest_Info
        GROUP BY pais, intereses
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)


@app.route('/api/proveedores', methods=['GET'])
def obtener_proveedores():
    conn = get_db_connection()
    query = """
        SELECT nombre_empresa, COUNT(*) as total
        FROM Provider_Info
        GROUP BY nombre_empresa
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/turistas', methods=['GET'])
def obtener_turistas():
    conn = get_db_connection()
    query = """
        SELECT COUNT(*) as total FROM Turist_Info
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultado = cursor.fetchone()
    conn.close()
    return jsonify(resultado)

@app.route('/api/turistas-genero', methods=['GET'])
def turistas_por_genero():
    conn = get_db_connection()
    query = """
        SELECT genero, COUNT(*) as total
        FROM Turist_Info
        GROUP BY genero
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/servicios-mas-solicitados', methods=['GET'])
def servicios_mas_solicitados():
    conn = get_db_connection()
    query = """
        SELECT intereses, COUNT(*) as total
        FROM Service_Search_By_UserStatus
        GROUP BY intereses
        ORDER BY total DESC
        LIMIT 10
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/top-proveedores-activos', methods=['GET'])
def top_proveedores_activos():
    conn = get_db_connection()
    query = """
        SELECT p.nombre_empresa, COUNT(*) as total
        FROM Provider_Info p
        JOIN Service_Location_Info s ON p.id = s.proveedor_id
        WHERE s.estado = 1
        GROUP BY p.nombre_empresa
        ORDER BY total DESC
        LIMIT 5
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

if __name__ == '__main__':
    app.run(debug=True)

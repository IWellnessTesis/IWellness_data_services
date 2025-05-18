from flask import Flask, jsonify, request
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

@app.route('/api/dashboard-proveedor', methods=['GET'])
def dashboard_proveedor():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Total de servicios de este proveedor
    query_total = "SELECT COUNT(*) as total FROM Service_Location_Info WHERE idProveedor = %s"
    query_activos = "SELECT COUNT(*) as activos FROM Service_Location_Info WHERE idProveedor = %s AND estado = 1"
    query_inactivos = "SELECT COUNT(*) as inactivos FROM Service_Location_Info WHERE idProveedor = %s AND estado = 0"
    query_nombre = "SELECT nombre_empresa FROM Provider_Info WHERE idProveedor = %s"

    cursor.execute(query_total, (idProveedor,))
    total = cursor.fetchone()['total']
    cursor.execute(query_activos, (idProveedor,))
    activos = cursor.fetchone()['activos']
    cursor.execute(query_inactivos, (idProveedor,))
    inactivos = cursor.fetchone()['inactivos']
    cursor.execute(query_nombre, (idProveedor,))
    nombre_row = cursor.fetchone()
    nombre = nombre_row['nombre_empresa'] if nombre_row else "Sin nombre"

    conn.close()
    return jsonify({
        'nombre_empresa': nombre,
        'total_servicios': total,
        'activos': activos,
        'inactivos': inactivos
    })

@app.route('/api/dashboard-admin', methods=['GET'])
def dashboard_admin():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query_total = "SELECT COUNT(*) as total FROM Service_Location_Info"
    query_activos = "SELECT COUNT(*) as activos FROM Service_Location_Info WHERE estado = 1"
    query_inactivos = "SELECT COUNT(*) as inactivos FROM Service_Location_Info WHERE estado = 0"

    cursor.execute(query_total)
    total = cursor.fetchone()['total']
    cursor.execute(query_activos)
    activos = cursor.fetchone()['activos']
    cursor.execute(query_inactivos)
    inactivos = cursor.fetchone()['inactivos']

    conn.close()
    return jsonify({
        'nombre_empresa': 'Todos',
        'total_servicios': total,
        'activos': activos,
        'inactivos': inactivos
    })

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
    JOIN Service_Location_Info s ON p.id = s.idProveedor
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

@app.route('/api/turistas-nacionalidad', methods=['GET'])
def turistas_por_nacionalidad():
    conn = get_db_connection()
    query = """
        SELECT pais as name, COUNT(*) as value
        FROM Turist_Info
        GROUP BY pais
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/reservas', methods=['GET'])
def obtener_reservas():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT r.*, s.serviceName
        FROM Book_Service_Info r
        LEFT JOIN Service_Location_Info s ON r._idServicio = s.serviceid
    """
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/total-servicios', methods=['GET'])
def proveedor_total_servicios():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT COUNT(*) as total_servicios FROM Service_Location_Info WHERE idProveedor = %s"
    cursor.execute(query, (idProveedor,))
    resultado = cursor.fetchone()
    conn.close()
    return jsonify(resultado)

@app.route('/api/proveedor/lista-servicios', methods=['GET'])
def proveedor_lista_servicios():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = "SELECT id, serviceName, estado FROM Service_Location_Info WHERE idProveedor = %s"
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/total-reservas', methods=['GET'])
def proveedor_total_reservas():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT COUNT(*) as total_reservas
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        WHERE s.idProveedor = %s
    """
    cursor.execute(query, (idProveedor,))
    resultado = cursor.fetchone()
    conn.close()
    return jsonify(resultado)

@app.route('/api/proveedor/reservas-por-servicio', methods=['GET'])
def proveedor_reservas_por_servicio():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT s.serviceName, COUNT(*) as total_reservas
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        WHERE s.idProveedor = %s
        GROUP BY s.serviceName
        ORDER BY total_reservas DESC
    """
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/reservas-por-estado', methods=['GET'])
def proveedor_reservas_por_estado():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT r.estado, COUNT(*) as total
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        WHERE s.idProveedor = %s
        GROUP BY r.estado
    """
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/reservas-por-genero', methods=['GET'])
def proveedor_reservas_por_genero():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT t.genero, COUNT(*) as total
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        JOIN Turist_Info t ON r._idTurista = t.idTurista
        WHERE s.idProveedor = %s
        GROUP BY t.genero
    """
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/reservas-por-nacionalidad', methods=['GET'])
def proveedor_reservas_por_nacionalidad():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT t.pais, COUNT(*) as total
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        JOIN Turist_Info t ON r._idTurista = t.idTurista
        WHERE s.idProveedor = %s
        GROUP BY t.pais
    """
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)

@app.route('/api/proveedor/reservas-por-estado-civil', methods=['GET'])
def proveedor_reservas_por_estado_civil():
    idProveedor = request.args.get('idProveedor', default=None, type=int)
    if not idProveedor:
        return jsonify({'error': 'idProveedor es requerido'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT t.estadoCivil, COUNT(*) as total
        FROM Book_Service_Info r
        JOIN Service_Location_Info s ON r._idServicio = s.serviceid
        JOIN Turist_Info t ON r._idTurista = t.idTurista
        WHERE s.idProveedor = %s
        GROUP BY t.estadoCivil
        ORDER BY total DESC
    """
    cursor.execute(query, (idProveedor,))
    resultados = cursor.fetchall()
    conn.close()
    return jsonify(resultados)


if __name__ == '__main__':
    app.run(debug=True)

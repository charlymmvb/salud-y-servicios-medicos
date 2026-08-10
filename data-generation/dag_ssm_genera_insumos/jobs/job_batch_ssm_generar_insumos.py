import argparse
import logging
import json

import hashlib
import numpy as np

from collections import OrderedDict
from datetime import datetime, time, date, timedelta
from faker import Faker
from pyspark.sql import SparkSession

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def read_json(ruta_json, proyecto):
    ruta = ruta_json.replace("gs://", "")
    bucket_name, file_name = ruta.split("/", 1)

    cliente_storage = storage.Client(project=proyecto)
    bucket = cliente_storage.bucket(bucket_name)
    blob = bucket.blob(file_name)

    return json.loads(blob.download_as_text())

def convertir_claves_int(diccionario):
    return {int(clave): valor for clave, valor in diccionario.items()}

def generar_matriz_camas(camas_limit, red_sedes_len):
    matriz = np.zeros((red_sedes_len, 4), dtype=int)

    total = matriz.size

    mean = int(camas_limit / (total))

    count_camas = 0

    for i in range(matriz.shape[0]):
        for j in range(matriz.shape[1]):

            casillas_restantes = total - (4 * i + j)    #1
            camas_restantes = camas_limit - count_camas #1

            camas_max = camas_restantes - casillas_restantes + 1  #1

            camas_rand = np.abs(int(np.random.normal(loc=mean, scale=500)))   #500
            camas = max(1, min(camas_rand, camas_max))             #1

            if i==matriz.shape[0]-1 and j==matriz.shape[1]-1:
                camas = camas_restantes

            matriz[i][j] = camas
            count_camas += camas
    return matriz

if _name_ == "_main_":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init_json", required=True, help="JSON config file (GCS)")
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    config = read_json(args.init_json, args.project)

    spark = (SparkSession.builder.appName("generar-pacientes").getOrCreate())

    fake = Faker()

    #Lectura de parametros
    parametros = config["parametros"]

    seed = parametros["seed"]
    hr_min_atencion = parametros["hr_min_atencion"]
    max_dias_atencion = parametros["max_dias_atencion"]
    max_intentos = parametros["max_intentos"]

    hr_min_consultas = parametros["hr_min_consultas"]
    hr_max_consultas = parametros["hr_max_consultas"]

    t_reser_consulta_mean = parametros["t_reser_consulta_mean"]
    t_reser_consulta_limit = parametros["t_reser_consulta_limit"]

    info = config["info"]

    alta_complejidad_limit = info["alta_complejidad_limit"]
    t_consulta_externa_mean = info["t_consulta_externa_mean"]
    t_consulta_externa_limit = info["t_consulta_externa_limit"]
    fecha_fundacion = info["fecha_fundacion"]

    tablas = config["tablas"]

    bucket = tablas["bucket"]
    pac_registro_len = tablas["pac_registro_len"]
    med_planta_len = tablas["med_planta_len"]
    red_sedes_len = tablas["red_sedes_len"]
    hce_encuentros_len = tablas["hce_encuentros_len"]
    gcm_camas_len = tablas["gcm_camas_len"]
    far_dispensacion_len = tablas["far_dispensacion_len"]
    age_citas_len = tablas["age_citas_len"]

    pac_registro_cols = tablas["pac_registro_cols"]
    med_planta_cols = tablas["med_planta_cols"]
    red_sedes_cols = tablas["red_sedes_cols"]
    hce_encuentros_cols = tablas["hce_encuentros_cols"]
    gcm_camas_cols = tablas["gcm_camas_cols"]
    far_dispensacion_cols = tablas["far_dispensacion_cols"]
    age_citas_cols = tablas["age_citas_cols"]

    pac_registro = config["pac_registro"]

    docs_col = pac_registro["docs_col"]
    docs_ecu = pac_registro["docs_ecu"]
    docs_peru = pac_registro["docs_peru"]

    ext_peru = pac_registro["ext_peru"]
    ext_gral = pac_registro["ext_gral"]

    # Convertir "null" del JSON a None de Python
    generos = { None if clave == "null" else clave: valor for clave, valor in pac_registro["generos"].items()}
    estratos = { None if clave == "null" else clave: valor for clave, valor in pac_registro["estratos"].items()}

    tips_aseguradoras = pac_registro["tips_aseguradoras"]

    min_age = pac_registro["min_age"]
    max_age = pac_registro["max_age"]

    ciudades = convertir_claves_int(pac_registro["ciudades"])
    eps_list = convertir_claves_int(pac_registro["eps_list"])

    med_planta = config["med_planta"]

    tips_contratos = {None if clave == "null" else clave: valor for clave, valor in med_planta["tips_contratos"].items()}
    tips_jornadas = {None if clave == "null" else clave: valor for clave, valor in med_planta["tips_jornadas"].items()}

    red_sedes = config["red_sedes"]

    tips_sedes = red_sedes["tips_sedes"]
    tips_unidades = red_sedes["tips_unidades"]
    camas_limit = red_sedes["camas_limit"]

    especialidades = convertir_claves_int(config["especialidades"])

    cie10 = config["cie10"]

    procedimientos = config["procedimientos"]

    vrs = config["vrs"]

    medicamentos = config["medicamentos"]

    otros = config["otros"]

    max_cant_med = otros["max_cant_med"]
    max_dias_med = otros["max_dias_med"]
    max_frec_med = otros["max_frec_med"]

    tips_prescripcion = otros["tips_prescripcion"]

    min_age_med = med_planta["min_age_med"]
    max_age_med = med_planta["max_age_med"]

    Faker.seed(seed)
    np.random.seed(seed)

    
    fec_fund = datetime.strptime(fecha_fundacion, '%Y-%m-%d').date()

    #GENERACION DE PAC REGISTRO
    pac_registro = []

    for i in range(pac_registro_len):
        pac_id=i+1

        id_ciudad_res = fake.random_int(min=1, max=168)
        ciudad = ciudades[id_ciudad_res]

        edad = int(np.random.normal(loc=45, scale=15))
        while edad < min_age or edad > max_age:
            edad = int(np.random.normal(loc=45, scale=15))
        hoy = date.today()
        dias_extra = np.random.randint(0, 365)
        fec_nac = hoy.replace(year=hoy.year - edad) - timedelta(days=dias_extra)

        if edad<7:
            tip_doc="Identificacion de menores"
            if id_ciudad_res <= 116:
                num_doc = fake.random_number(digits=ext_gral)
            else:
                num_doc = fake.random_number(digits=ext_peru)

        elif id_ciudad_res <= 96:
            tip_doc = fake.random_element(docs_col)
            num_doc = fake.random_number(digits=ext_gral)
        elif id_ciudad_res <= 116:
            tip_doc = fake.random_element(docs_ecu)
            num_doc = fake.random_number(digits=ext_gral)
        elif id_ciudad_res <= 168:
            tip_doc = fake.random_element(docs_peru)
            num_doc = fake.random_number(digits=ext_peru)
        if tip_doc == "Pasaporte":
            num_doc = fake.passport_number()

        num_doc_hash = hashlib.sha256(str(num_doc).encode('utf-8')).hexdigest()

        genero = fake.random_element(generos)

        tip_aseguradora = fake.random_element(tips_aseguradoras)

        if tip_aseguradora is None:
            id_eps = None
            eps = None
        else:
            id_eps = fake.random_int(min=1, max=18)
            eps = eps_list[id_eps]

        estrato_socioec = fake.random_element(estratos)

        if fec_nac < fec_fund:
            fec_primer_atencion = fake.date_between(start_date=fec_fund, end_date='today')
        else:
            fec_primer_atencion = fake.date_between(start_date=fec_nac, end_date='today')

        activo = fake.boolean(chance_of_getting_true=85)

        pac_registro.append({
            "pac_id": pac_id,
            "tip_doc": tip_doc,
            "num_doc_hash": num_doc_hash,
            "fec_nac": fec_nac,
            "genero": genero,
            "id_ciudad_res": id_ciudad_res,
            "ciudad_res": ciudad,
            "tip_aseguradora": tip_aseguradora,
            "id_eps": id_eps,
            "eps": eps,
            "estrato_socioec": estrato_socioec,
            "fec_primer_atencion": fec_primer_atencion,
            "activo": activo
        })

    df_pac_registro = spark.createDataFrame(pac_registro)

    df_pac_registro = df_pac_registro.select(*pac_registro_cols)

    #GENERACION DE MED PLANTA
    
    med_planta = []

    for i in range(med_planta_len):
        med_id=i+1

        n_esp = fake.random_int(min=1, max=20)

        especialidad = especialidades[n_esp]

        esp_principal = especialidad['especialidad']

        esp_secundaria = fake.random_element(especialidad['subespecialidades'])

        id_sede = fake.random_int(min=1, max=red_sedes_len)

        edad = int(np.random.normal(loc=45, scale=15))
        while edad < min_age_med or edad > max_age_med:
            edad = int(np.random.normal(loc=45, scale=15))
        hoy = date.today()
        fec_min = hoy.replace(year=hoy.year - edad + min_age_med)

        if fec_min < fec_fund:
            fec_ingreso = fake.date_between(start_date=fec_fund, end_date='today')
        else:
            fec_ingreso = fake.date_between(start_date=fec_min, end_date='today')

        tip_contrato = fake.random_element(tips_contratos)

        jornada = fake.random_element(tips_jornadas)

        estado_activo = fake.boolean(chance_of_getting_true=95)

        med_planta.append({
                "med_id": med_id,
                "esp_principal": esp_principal,
                "esp_secundaria": esp_secundaria,
                "id_sede": id_sede,
                "fec_ingreso": fec_ingreso,
                "tip_contrato": tip_contrato,
                "jornada": jornada,
                "estado_activo": estado_activo
        })

    df_med_planta = spark.createDataFrame(med_planta)

    df_med_planta = df_med_planta.select(*med_planta_cols)

    #GENERACION DE RED SEDES
    red_sedes = []
    fake.unique.clear()

    tips_sedes_aux = []

    for tipo, cantidad in tips_sedes.items():
        tips_sedes_aux.extend([tipo] * cantidad)

    np.random.shuffle(tips_sedes_aux)

    matriz = generar_matriz_camas(camas_limit, red_sedes_len)

    for i in range(red_sedes_len):
        id_sede = i+1

        id_ciudad = fake.unique.random_int(min=1, max=168)
        nom_sede = ciudades[id_ciudad]

        tip_sede = tips_sedes_aux[i]

        if id_ciudad <= 96:
            id_pais="Colombia"
        elif id_ciudad <= 116:
            id_pais="Ecuador"
        elif id_ciudad <= 168:
            id_pais="Peru"

        cap_camas_gen = int(matriz[i][0])
        cap_camas_uci = int(matriz[i][1])
        cap_camas_cirugia = int(matriz[i][2])
        cap_camas_urg = int(matriz[i][3])

        if tip_sede == "Hospital de alta complejidad":
            nivel_complejidad="Alta"
        elif tip_sede == "Clinica de mediana complejidad":
            nivel_complejidad="Mediana"
        else:
            nivel_complejidad="Baja"

        red_sedes.append({
            "id_sede": id_sede,
            "nom_sede": nom_sede,
            "tip_sede": tip_sede,
            "id_ciudad": id_ciudad,
            "id_pais": id_pais,
            "cap_camas_gen": cap_camas_gen,
            "cap_camas_uci": cap_camas_uci,
            "cap_camas_cirugia": cap_camas_cirugia,
            "cap_camas_urg": cap_camas_urg,
            "nivel_complejidad": nivel_complejidad
        })

    df_red_sedes = spark.createDataFrame(red_sedes)

    df_red_sedes = df_red_sedes.select(*red_sedes_cols)

    df_red_sedes.show()

    #GENERACION DE HCE_ENCUENTROS
    fake.unique.clear()
    hce_encuentros = []
    restantes = hce_encuentros_len
    primeros_ingresos = []
    id_enc = 1

    #Primer ingreso
    #Aprox 27% de los pacientes tuvo hospitalizacion en su primera atencion (Esto es arbitrario)
    for i in range(int(pac_registro_len*0.27)):
        id_encuentro = id_enc
        id_enc += 1

        pac_id = fake.unique.random_int(min=1, max=pac_registro_len)
        df_pac_aux = df_pac_registro.filter(df_pac_registro.pac_id == pac_id)

        prim_atencion = df_pac_aux.select('fec_primer_atencion').first()["fec_primer_atencion"]

        df_med_aux1 = df_med_planta.filter(df_med_planta.fec_ingreso <= prim_atencion)

        med_ids = [row["med_id"] for row in df_med_aux1.select("med_id").collect()]
        med_id = fake.random_element(med_ids)
        df_med_aux = df_med_planta.filter(df_med_planta.med_id == med_id)

        id_sede = df_med_aux.select('id_sede').first()[0]

        prim_atencion_dt = datetime.combine(prim_atencion,datetime.min.time())

        fec_inicio_atencion = fake.date_time_between_dates(datetime_start=prim_atencion_dt, datetime_end=prim_atencion_dt + timedelta(days=1) - timedelta(seconds=1))
        tiempo_espera = np.random.exponential(scale=t_consulta_externa_mean)
        tiempo_espera = min(tiempo_espera, t_consulta_externa_limit)
        fec_registro = fec_inicio_atencion - timedelta(minutes=tiempo_espera)
        fec_egreso = fake.date_time_between_dates( datetime_start=fec_inicio_atencion, datetime_end=fec_inicio_atencion + timedelta(days=max_dias_atencion))

        tip_consulta = "Primera vez"

        esp_atendida = df_med_aux.select('esp_principal').first()[0]

        posibles_cie10 = cie10[esp_atendida].copy()

        diag_principal_cie10 = fake.random_element(posibles_cie10)

        posibles_cie10.remove(diag_principal_cie10)

        if posibles_cie10 and fake.boolean(chance_of_getting_true=20):
            diag_sec1_cie10 = fake.random_element(posibles_cie10)
        else:
            diag_sec1_cie10 = None

        cod_procedimientos = procedimientos[esp_atendida]
        vr_facturado = vrs[cod_procedimientos]

        if fake.boolean(chance_of_getting_true=95):
            estado_factura = "Pagado"
        else:
            estado_factura = "No pagado"

        hce_encuentros.append({
            "id_encuentro": id_encuentro,
            "pac_id": pac_id,
            "med_id": med_id,
            "id_sede": id_sede,
            "fec_registro": fec_registro,
            "fec_inicio_atencion": fec_inicio_atencion,
            "fec_egreso": fec_egreso,
            "tip_consulta": tip_consulta,
            "esp_atendida": esp_atendida,
            "diag_principal_cie10": diag_principal_cie10,
            "diag_sec1_cie10": diag_sec1_cie10,
            "cod_procedimientos": cod_procedimientos,
            "vr_facturado": vr_facturado,
            "estado_factura": estado_factura
        })

        primeros_ingresos.append(pac_id)
        restantes -= 1


    #Despues del primer ingreso

    while restantes>0:
        id_encuentro = id_enc
        id_enc += 1

        med_ids = [row["med_id"] for row in df_med_planta.select("med_id").collect()]
        med_id = fake.random_element(med_ids)
        df_med_aux = df_med_planta.filter(df_med_planta.med_id == med_id)
        prim_med = df_med_aux.select('fec_ingreso').first()["fec_ingreso"]
        id_sede = df_med_aux.select('id_sede').first()[0]

        df_pac_aux = df_pac_registro.filter(df_pac_registro.fec_primer_atencion >= prim_med)

        pac_ids = [row["pac_id"] for row in df_pac_aux.select("pac_id").collect()]
        pac_id = fake.random_element(pac_ids)
        prim_atencion = df_pac_aux.filter(df_pac_aux.pac_id == pac_id).first()["fec_primer_atencion"]


        fecha_minima = max(prim_atencion + timedelta(days=max_dias_atencion + 1), prim_med)
        if fecha_minima > date.today():
            continue

        fecha_minima_dt = datetime.combine(fecha_minima,datetime.min.time())
        hoy_dt = datetime.combine(date.today(),datetime.max.time())
        fec_inicio_atencion = fake.date_time_between_dates(datetime_start=fecha_minima_dt,datetime_end=hoy_dt)
        tiempo_espera = np.random.exponential(scale=t_consulta_externa_mean)
        tiempo_espera = min(tiempo_espera, t_consulta_externa_limit)
        fec_registro = fec_inicio_atencion - timedelta(minutes=tiempo_espera)
        fec_egreso = fake.date_time_between_dates(datetime_start=fec_inicio_atencion,datetime_end=fec_inicio_atencion + timedelta(days=max_dias_atencion))

        tip_consulta = "Control"

        esp_atendida = df_med_aux.select('esp_principal').first()[0]

        posibles_cie10 = cie10[esp_atendida].copy()

        diag_principal_cie10 = fake.random_element(posibles_cie10)

        posibles_cie10.remove(diag_principal_cie10)

        if posibles_cie10 and fake.boolean(chance_of_getting_true=20):
            diag_sec1_cie10 = fake.random_element(posibles_cie10)
        else:
            diag_sec1_cie10 = None

        cod_procedimientos = procedimientos[esp_atendida]
        vr_facturado = vrs[cod_procedimientos]

        if fake.boolean(chance_of_getting_true=95):
            estado_factura = "Pagado"
        else:
            estado_factura = "No pagado"

        hce_encuentros.append({
            "id_encuentro": id_encuentro,
            "pac_id": pac_id,
            "med_id": med_id,
            "id_sede": id_sede,
            "fec_registro": fec_registro,
            "fec_inicio_atencion": fec_inicio_atencion,
            "fec_egreso": fec_egreso,
            "tip_consulta": tip_consulta,
            "esp_atendida": esp_atendida,
            "diag_principal_cie10": diag_principal_cie10,
            "diag_sec1_cie10": diag_sec1_cie10,
            "cod_procedimientos": cod_procedimientos,
            "vr_facturado": vr_facturado,
            "estado_factura": estado_factura
        })

        restantes -= 1

    df_hce_encuentros = spark.createDataFrame(hce_encuentros)
    df_hce_encuentros = df_hce_encuentros.select(*hce_encuentros_cols)

    #GENERACION DE GCM_CAMAS
    #Se obtienen las horas que necesitaremos, y con ello la fecha de inicio y fin con horas
    horas = int((gcm_camas_len / (len(red_sedes)*4))+1)
    hora_final = datetime.now().replace(minute=0, second=0, microsecond=0)
    hora_inicial = hora_final - timedelta(hours= horas - 1)

    sedes = df_red_sedes.select("id_sede", "cap_camas_gen", "cap_camas_uci", "cap_camas_cirugia", "cap_camas_urg").collect()

    capacidades = {}

    for sede in sedes:
        id_sede = sede["id_sede"]
        capacidades[id_sede] = {
            "General": sede["cap_camas_gen"],
            "UCI": sede["cap_camas_uci"],
            "Cirugia": sede["cap_camas_cirugia"],
            "Urgencias": sede["cap_camas_urg"]
        }

    gcm_camas = []
    id_registro_cama = 1

    for i in range(horas):

        hora_actual = hora_inicial + timedelta(hours=i)

        encuentros_activos = df_hce_encuentros.filter((df_hce_encuentros.fec_inicio_atencion <= hora_actual)
                                                & (df_hce_encuentros.fec_egreso > hora_actual))

        for id_sede, capacidades_sede in capacidades.items():

            pacientes_sede = encuentros_activos.filter(encuentros_activos.id_sede == id_sede).select("id_encuentro").collect()
            camas_disponibles = capacidades_sede.copy()
            ocupadas = {"General": 0, "UCI": 0, "Cirugia": 0, "Urgencias": 0}

            for paciente in pacientes_sede:
                tipos_disponibles = []
                for tipo in tips_unidades:
                    if camas_disponibles[tipo] > 0:
                        tipos_disponibles.append(tipo)

                if len(tipos_disponibles) == 0:
                    break

                tipo_elegido = fake.random_element(tipos_disponibles)

                ocupadas[tipo_elegido] += 1
                camas_disponibles[tipo_elegido] -= 1

            for tipo in tips_unidades:

                capacidad = capacidades_sede[tipo]
                num_camas_ocupadas = ocupadas[tipo]
                num_camas_mant = 0
                num_camas_disp = (capacidad - num_camas_ocupadas - num_camas_mant)

                num_camas_mant = fake.random_int(min=0, max=int(0.10*num_camas_disp))

                if num_camas_mant > 0:
                    motivo_indisponibilidad = "Mantenimiento"
                else:
                    motivo_indisponibilidad = ""

                gcm_camas.append({
                    "id_registro_cama": id_registro_cama,
                    "id_sede": id_sede,
                    "tip_unidad": tipo,
                    "fec_hora_registro": hora_actual,
                    "num_camas_ocupadas": num_camas_ocupadas,
                    "num_camas_disp": num_camas_disp,
                    "num_camas_mant": num_camas_mant,
                    "motivo_indisponibilidad": motivo_indisponibilidad
                })

                id_registro_cama += 1



    df_gcm_camas = spark.createDataFrame(gcm_camas)
    df_gcm_camas = df_gcm_camas.select(*gcm_camas_cols)

    #GENERAR FAR_DISPENSACION
    far_dispensacion = []

    for i in range(far_dispensacion_len):
        id_dispensacion = i + 1

        id_encuentro = fake.random_int(min=1, max=df_hce_encuentros.count())
        pac_id = df_hce_encuentros.filter(df_hce_encuentros.id_encuentro == id_encuentro).select("pac_id").first()["pac_id"]
        id_sede = df_hce_encuentros.filter(df_hce_encuentros.id_encuentro == id_encuentro).select("id_sede").first()["id_sede"]

        fec_inicio_atencion_aux = df_hce_encuentros.filter(df_hce_encuentros.id_encuentro == id_encuentro).select("fec_inicio_atencion").first()["fec_inicio_atencion"]
        fec_egreso_aux = df_hce_encuentros.filter(df_hce_encuentros.id_encuentro == id_encuentro).select("fec_egreso").first()["fec_egreso"]
        fec_dispensacion = fake.date_time_between_dates(datetime_start=fec_inicio_atencion_aux, datetime_end=fec_egreso_aux)

        cod_medicamento = fake.random_element(medicamentos.keys())
        nombre_medicamento = medicamentos[cod_medicamento]["nombre"]
        vr_unitario = medicamentos[cod_medicamento]["precio"]
        cantidad = fake.random_int(min=1, max=max_cant_med)

        if fake.boolean(chance_of_getting_true=95):
            tip_prescripcion = tips_prescripcion[1]
        else:
            tip_prescripcion = tips_prescripcion[0]

        far_dispensacion.append({
                "id_dispensacion": id_dispensacion,
                "id_encuentro": id_encuentro,
                "pac_id": pac_id,
                "id_sede": id_sede,
                "fec_dispensacion": fec_dispensacion,
                "cod_medicamento": cod_medicamento,
                "nombre_medicamento": nombre_medicamento,
                "cantidad": cantidad,
                "vr_unitario": vr_unitario,
                "tip_prescripcion": tip_prescripcion
            })

    df_far_dispensacion = spark.createDataFrame(far_dispensacion)
    df_far_dispensacion = df_far_dispensacion.select(*far_dispensacion_cols)

    #GENERAR AGE_CITAS
    
    fake.unique.clear()

    age_citas = []

    restantes = age_citas_len
    id_aux = 1
    n_intentos = 0

    #Primer ingreso
    for pac_id in range(1, pac_registro_len + 1):

        if(pac_id not in primeros_ingresos):
            id_cita = id_aux
            id_aux += 1

            df_pac_aux = df_pac_registro.filter(df_pac_registro.pac_id == pac_id)

            prim_atencion = df_pac_aux.select('fec_primer_atencion').first()["fec_primer_atencion"]

            df_med_aux1 = df_med_planta.filter(df_med_planta.fec_ingreso <= prim_atencion)

            med_ids = [row["med_id"] for row in df_med_aux1.select("med_id").collect()]
            med_id = fake.random_element(med_ids)
            df_med_aux = df_med_planta.filter(df_med_planta.med_id == med_id)

            id_sede = df_med_aux.select('id_sede').first()[0]

            prim_atencion_dt = datetime.combine(prim_atencion, time(hr_min_consultas,0))

            fec_cita_programada = fake.date_time_between_dates(datetime_start = prim_atencion_dt, datetime_end=prim_atencion_dt.replace(hour = hr_max_consultas))
            tiempo_reser = np.random.exponential(scale=t_reser_consulta_mean)
            tiempo_reser = min(tiempo_reser, t_reser_consulta_limit)
            fec_agendamiento = fec_cita_programada - timedelta(hours=tiempo_reser)

            fec_cita_programada_date = fec_cita_programada.date()
            hra_cita_programada = fec_cita_programada.time()

            if fec_cita_programada_date > date.today():
                hra_llegada_paciente = hra_cita_programada
            else:
                tiempo_espera = int(np.random.exponential(scale=t_consulta_externa_mean))
                tiempo_espera = min(tiempo_espera, t_consulta_externa_limit)

                tiempo_espera = fake.random_int(min=-tiempo_espera, max=tiempo_espera)

                fec_llegada_paciente = fec_cita_programada - timedelta(minutes=tiempo_espera)
                hra_llegada_paciente = fec_llegada_paciente.time()

            hra_inicio_atencion = max(hra_cita_programada, hra_llegada_paciente)

            tip_cita = "Primera vez"

            esp_solicitada = df_med_aux.select('esp_principal').first()[0]

            if fec_cita_programada_date > date.today():
                estado_cita = "Agendada"
            elif hra_inicio_atencion.hour >= 22:
                estado_cita = "Cancelada"
            else:
                estado_cita = "Atendida"

            age_citas.append({
                "id_cita": id_cita,
                "pac_id": pac_id,
                "med_id": med_id,
                "id_sede": id_sede,
                "fec_agendamiento": fec_agendamiento,
                "fec_cita_programada": fec_cita_programada,
                "hra_cita_programada": hra_cita_programada.strftime("%H:%M:%S"),
                "hra_llegada_paciente": hra_llegada_paciente.strftime("%H:%M:%S"),
                "hra_inicio_atencion": hra_inicio_atencion.strftime("%H:%M:%S"),
                "esp_solicitada": esp_solicitada,
                "tip_cita": tip_cita,
                "estado_cita": estado_cita
            })

            primeros_ingresos.append(pac_id)
            restantes -= 1


    #Despues del primer ingreso

    while restantes>0 and n_intentos<max_intentos:
        id_cita = id_aux
        id_aux += 1

        med_ids = [row["med_id"] for row in df_med_planta.select("med_id").collect()]
        med_id = fake.random_element(med_ids)
        df_med_aux = df_med_planta.filter(df_med_planta.med_id == med_id)
        prim_med = df_med_aux.select('fec_ingreso').first()["fec_ingreso"]
        id_sede = df_med_aux.select('id_sede').first()[0]

        df_pac_aux = df_pac_registro.filter(df_pac_registro.fec_primer_atencion >= prim_med)

        pac_ids = [row["pac_id"] for row in df_pac_aux.select("pac_id").collect()]
        if not pac_ids:
            n_intentos += 1
            continue
        pac_id = fake.random_element(pac_ids)
        prim_atencion = df_pac_aux.filter(df_pac_aux.pac_id == pac_id).first()["fec_primer_atencion"]
        fecha_minima = prim_atencion + timedelta(days=1)
        if fecha_minima > date.today():
            n_intentos += 1
            continue
        prim_atencion_dt = datetime.combine(fecha_minima, time(hr_min_consultas,0))

        fec_cita_programada = fake.date_time_between_dates(datetime_start = prim_atencion_dt, datetime_end='today')
        tiempo_reser = np.random.exponential(scale=t_reser_consulta_mean)
        tiempo_reser = min(tiempo_reser, t_reser_consulta_limit)
        fec_agendamiento = fec_cita_programada - timedelta(hours=tiempo_reser)

        fec_cita_programada_date = fec_cita_programada.date()
        hra_cita_programada = fec_cita_programada.time()

        if fec_cita_programada_date > date.today():
            hra_llegada_paciente = hra_cita_programada
        else:
            tiempo_espera = int(np.random.exponential(scale=t_consulta_externa_mean))
            tiempo_espera = min(tiempo_espera, t_consulta_externa_limit)

            tiempo_espera = fake.random_int(min=-tiempo_espera, max=tiempo_espera)

            fec_llegada_paciente = fec_cita_programada - timedelta(minutes=tiempo_espera)
            hra_llegada_paciente = fec_llegada_paciente.time()

        hra_inicio_atencion = max(hra_cita_programada, hra_llegada_paciente)

        tip_cita = "Control"

        esp_solicitada = df_med_aux.select('esp_principal').first()[0]

        if fec_cita_programada_date > date.today():
            estado_cita = "Agendada"
        elif hra_inicio_atencion.hour >= 22:
            estado_cita = "Cancelada"
        else:
            estado_cita = "Atendida"

        age_citas.append({
            "id_cita": id_cita,
            "pac_id": pac_id,
            "med_id": med_id,
            "id_sede": id_sede,
            "fec_agendamiento": fec_agendamiento,
            "fec_cita_programada": fec_cita_programada,
            "hra_cita_programada": hra_cita_programada.strftime("%H:%M:%S"),
            "hra_llegada_paciente": hra_llegada_paciente.strftime("%H:%M:%S"),
            "hra_inicio_atencion": hra_inicio_atencion.strftime("%H:%M:%S"),
            "esp_solicitada": esp_solicitada,
            "tip_cita": tip_cita,
            "estado_cita": estado_cita
        })

        restantes -= 1

    df_age_citas = spark.createDataFrame(age_citas)
    df_age_citas = df_age_citas.select(*age_citas_cols)

    logger.info("Iniciando escritura de archivos en storage")

    logger.info(f"Escribiendo df_pac_registro en ruta : {bucket}/pac_registro/data")
    df_pac_registro.write.mode("overwrite").format("parquet").save(f"{bucket}/pac_registro/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_med_planta en ruta : {bucket}/med_planta/data")
    df_med_planta.write.mode("overwrite").format("avro").save(f"{bucket}/med_planta/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_red_sedes en ruta : {bucket}/red_sedes/data")
    df_red_sedes.write.mode("overwrite").format("csv").save(f"{bucket}/red_sedes/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_hce_encuentros en ruta : {bucket}/hce_encuentros/data")
    df_hce_encuentros.write.mode("overwrite").format("parquet").save(f"{bucket}/hce_encuentros/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_gcm_camas en ruta : {bucket}/gcm_camas/data")
    df_gcm_camas.write.mode("overwrite").format("avro").save(f"{bucket}/gcm_camas/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_far_dispensacion en ruta : {bucket}/far_dispensacion/data")
    df_far_dispensacion.write.mode("overwrite").format("avro").save(f"{bucket}/far_dispensacion/data")
    logger.info(f"Tabla escrita correctamente")

    logger.info(f"Escribiendo df_age_citas en ruta : {bucket}/age_citas/data")
    df_age_citas.write.mode("overwrite").format("parquet").save(f"{bucket}/age_citas/data")
    logger.info(f"Tabla escrita correctamente")

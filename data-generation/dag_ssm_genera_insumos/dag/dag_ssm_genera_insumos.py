import pytz as tz

from airflow import models
from datetime import datetime , timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.hooks.gcs import parse_json_from_gcs
from airflow.utils.email import send_email


def on_failure_correo(context):
  dag_id = context["dag"].dag_id
  task_id = context["task_instance"].task_id
  fecha_ejecucion = context["logical_date"]
  excepcion = context.get("exception")

  asunto = f"Fallo en ejecucion de DAG: {dag_id}"

  cuerpo = f"""
  <html>
    <body>
      <h3>Fallo en ejecucion de DAG</h3>

      <p><b>DAG:</b> {dag_id}</p>
      <p><b>Tarea:</b> {task_id}</p>
      <p><b>Fecha de ejecución:</b> {fecha_ejecucion}</p>
      <p><b>Error:</b> {excepcion}</p>
    </body>
  </html>
  """

  send_email(
    to=["carloscemendoza@gmail.com"],
    subject=asunto,
    html_content=cuerpo
  )

def on_success_ini_correo(context):
  dag_id = context["dag"].dag_id
  task_id = context["task_instance"].task_id
  fecha_ejecucion = context["logical_date"]

  asunto = f"Inicio de ejecucion de DAG: {dag_id}"

  cuerpo = f"""
  <html>
    <body>
      <h3>Inicio de ejecucion de DAG</h3>

      <p><b>DAG:</b> {dag_id}</p>
      <p><b>Tarea:</b> {task_id}</p>
      <p><b>Fecha de ejecución:</b> {fecha_ejecucion}</p>
    </body>
  </html>
  """

  send_email(
    to=["carloscemendoza@gmail.com"],
    subject=asunto,
    html_content=cuerpo
  )

def on_success_fin_correo(context):
  dag_id = context["dag"].dag_id
  task_id = context["task_instance"].task_id
  fecha_ejecucion = context["logical_date"]

  asunto = f"Ejecucion exitosa de DAG: {dag_id}"

  cuerpo = f"""
  <html>
    <body>
      <h3>Ejecucion exitosa de DAG</h3>

      <p><b>DAG:</b> {dag_id}</p>
      <p><b>Tarea:</b> {task_id}</p>
      <p><b>Fecha de ejecución:</b> {fecha_ejecucion}</p>
    </body>
  </html>
  """

  send_email(
    to=["carloscemendoza@gmail.com"],
    subject=asunto,
    html_content=cuerpo
  )


# Configuraciones de DAG obtenidas de Variables de Airflow en formato JSON
dag_config_airflow = Variable.get('settings_dag_ssm_genera_insumos', deserialize_json=True)
BUCKET_CONFIGURACION = dag_config_airflow['bucket_configuracion']
FILE_CONFIGURACION = dag_config_airflow['file_configuracion']
ENV = dag_config_airflow['project_env']
 
CONFIGURACION_DAG_ROUTE = f"""gs://{BUCKET_CONFIGURACION}/{FILE_CONFIGURACION}"""

# Leer json de configuracion
dag_config = parse_json_from_gcs(gcp_conn_id = "google_cloud_storage_default", file_uri = CONFIGURACION_DAG_ROUTE)

TIME = datetime.now().astimezone(tz.timezone('America/Mexico_City')).strftime("%Y-%m-%d-%H-%M-%S")

dag_config = dag_config[ENV]

PROJECT = dag_config["variables"]["project"]
REGION = dag_config["variables"]["region"]
SERVICE_ACCOUNT = dag_config["variables"]["sa"]

JOB_GENERA_INSUMOS = dag_config["job_batch_ssm_generar_insumos"]["python_file"]
CONFIG_GENERA_INSUMOS = dag_config["job_batch_ssm_generar_insumos"]["config_file"]
PROPERTIES_GENERA_INSUMOS = dag_config["job_batch_ssm_generar_insumos"]["properties"]

SPARK_DEPENDENCIES = dag_config["configs"]["spark_dependencies"]
SQL_DEPENDENCIES = dag_config["configs"]["sql_dependencies"]
FAKER_DEPENDENCIES = dag_config["configs"]["faker_dependencies"]
BATCH_ID1 = dag_config["configs"]["batch_id1"]
DAG_OWNER = dag_config["configs"]["dag_owner"]
RETRIES = dag_config["configs"]["retries"]
RETRIES_DELAY = dag_config["configs"]["retries_delay_minutes"]
MAX_ACTIVE_RUNS = dag_config["configs"]["max_active_runs"]
NUM_VERSION = dag_config["configs"]["rt_version"]
SCHEDULE_INTERVAL = None if dag_config["configs"]["schedule_interval"] == "" else dag_config["configs"]["schedule_interval"]
TAGS_JOB = dag_config["configs"]["tags"]


genera_insumos_id = f"""{BATCH_ID1}-{TIME}"""


python_files_uris = [
            f"""{FAKER_DEPENDENCIES}"""
        ]

environment_config = {
        "execution_config":{
                "service_account": SERVICE_ACCOUNT,
            }
    }

#properties = PROPERTIES_BATCH
jar_file_uris = [
            f"""{SPARK_DEPENDENCIES}""",
            f"""{SQL_DEPENDENCIES}"""
            
        ]


#Json de configuracion para el trabajo batch a ejecutar para los campos planos

args_genera_insumos = [
            "--init_json", f"""{CONFIG_GENERA_INSUMOS}""",
            "--project", TAGS_JOB[2],
        ]



JOB_BATCH_GENERA_INSUMOS = {
    "pyspark_batch": {
        "main_python_file_uri": f"""{JOB_GENERA_INSUMOS}""",
        "args": args_genera_insumos,
        "python_file_uris": python_files_uris,
        "jar_file_uris": jar_file_uris
    },
    "environment_config": environment_config,
    "runtime_config": {
        "version": NUM_VERSION,
        "properties": PROPERTIES_GENERA_INSUMOS
    }
}


default_dag_args = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': RETRIES,
    'retry_delay': timedelta(minutes = RETRIES_DELAY),
    'project':PROJECT,
    'max_active_runs': MAX_ACTIVE_RUNS,
    'on_failure_callback': on_failure_correo
}

#Definicion del DAG
with models.DAG(
    "dag_ssm_genera_insumos",
    default_args=default_dag_args,
    catchup=False,
    schedule= SCHEDULE_INTERVAL,
	tags=[TAGS_JOB[0]],
) as dag:
    
    #Tarea para indicar el inicio de la ejecucion del proceso
    inicio_dag = DummyOperator(
        task_id='inicio_dag',
        on_success_callback=on_success_ini_correo
    )
    
    job_batch_ssm_generar_insumos = DataprocCreateBatchOperator (
        task_id="job_batch_ssm_generar_insumos",
        project_id=PROJECT,
        region=REGION,
        batch=JOB_BATCH_GENERA_INSUMOS,
        batch_id=genera_insumos_id,
    )

    #Tarea para indicar el fin de la ejecucion del proceso
    fin_dag = DummyOperator(
        task_id='fin_dag',
        on_success_callback=on_success_fin_correo
    )

inicio_dag >> job_batch_ssm_generar_insumos >> fin_dag
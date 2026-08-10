# ESCENARIO ELEGIDO: SALUD Y SERVICIOS MEDICOS 
# Plataforma Escogida : Google Cloud Platform

**Justificacion:** El escenario de salud y servicios fue seleccionado debido a que anteriormente he trabajado en proyectos que involucran datos de polizas de seguros de gastos medicos, por lo que el entendimiento del negocio fue mas sencillo en comparacion con los otros casos. En cuanto a la plataforma elegida, el criterio fue similar, escogi GCP debido a que es la plataforma en la que tengo mas experiencia.

## FASE 1: Generacion de datos y modelo relacional

Para la generacion de datos aleatorios decidi implementar un dag, esto para simular un sistema legado del que se extraen los datos para depositarlos en storage, en formatos avro, parquet y csv.

El DAG cuenta con un unico job batch, que es el encargado de la generacion de los datos, disponible en data-generation\dag_ssm_genera_insumos\jobs\job_batch_ssm_generar_insumos.py
Los parametros para la generacion de datos se leen desde un archivo json ubicado en data-generation\dag_ssm_genera_insumos\jobs_config\config_job_batch_ssm_generar_insumos.json.

El proceso se baso en crear los dataframes en el orden de aparicion del documento, haciendo uno de numpy para datos que necesitaran distribuciones, spark para el manejo de datos y faker para la creacion de datos falsos, esta ultima se consulto en los siguientes portales:
* https://pypi.org/project/Faker/
* https://faker.readthedocs.io/en/master/providers/baseprovider.html#faker.providers.BaseProvider.random_element

Para la tabla PAC_REGISTRO el campo pac_id al ser un identificador se establecio como consecutivo, es decir, el primer registro obtuvo el 1, el segundo el 2, y asi sucesivamente. La fecha de nacimiento es un numero aleatorio, primero se calcula la edad de la persona siendo un numero aleatorio de entre 0 a 118 años (de acuerdo con la persona mas longeva registrada y que se puede atender personas con pocos meses de vida), siguiendo una distribucion normal con media de 45 años, debido a que a pesar de que la edad media de la poblacion en latinoamerica es de 30 años, conforme la edad aumenta muy probablemente tambien lo hagan los problemas de salud. (la informacion recopilada procede de https://www.eluniversal.com.mx/cartera/envejece-america-latina-cual-es-la-edad-media-de-los-latinoamericanos-y-cuantos-hijos-tienen/). En el caso de la ciudad se investigaron las ciudades de los 3 paises, se agregaron a un diccionario y se les asigno un ID, 

El tip_doc se establecio como tipo de documento, es decir, el documento que se utilizara para registrar la identidad del paciente. Dado que la empresa opera en 3 paises diferentes, investigue los documentos de identidad aceptados por cada uno de ellos (disponibles en https://help.clearid.io/ES/R_CID_IdentityDocumentTypes.html)




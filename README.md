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

Para la tabla PAC_REGISTRO el campo pac_id al ser un identificador se establecio como consecutivo, es decir, el primer registro obtuvo el 1, el segundo el 2, y asi sucesivamente. La fecha de nacimiento es un numero aleatorio, primero se calcula la edad de la persona siendo un numero aleatorio de entre 0 a 118 años (de acuerdo con la persona mas longeva registrada y que se puede atender personas con pocos meses de vida), siguiendo una distribucion normal con media de 45 años, debido a que a pesar de que la edad media de la poblacion en latinoamerica es de 30 años, conforme la edad aumenta muy probablemente tambien lo hagan los problemas de salud. (la informacion recopilada procede de https://www.eluniversal.com.mx/cartera/envejece-america-latina-cual-es-la-edad-media-de-los-latinoamericanos-y-cuantos-hijos-tienen/). En el caso de la ciudad se investigaron las ciudades de los 3 paises, se agregaron a un diccionario y se les asigno un ID, posteriormente para cada persona se eligio un numero aleatorio de entre las opciones.

El tip_doc se establecio como tipo de documento, es decir, el documento que se utilizara para registrar la identidad del paciente. Dado que la empresa opera en 3 paises diferentes, investigue los documentos de identidad aceptados por cada uno de ellos (disponibles en https://help.clearid.io/ES/R_CID_IdentityDocumentTypes.html), para la asignacion se realizo una validacion, buscando que el tipo de documento corresponda con la ciudad de residencia de la persona, y que las edades correspondieran, de esta manera no se podra tener una identificacion de menores para un mayor de edad, o una licencia de conducir para alguien con  años. Para generar el tipo de documento se utilizo fake, validando el numero de digitos que debia tener cada identificacion de acuerdo al pais, en el caso del pasaporte se utilizo la funcion de fake que permite generar pasaporte aleatorios. Una vez con el numero de documento, se implemento sha256 para obtener el hash para la columna num_doc_hash a traves de hashlib (https://www.w3schools.com/python/ref_module_hashlib.asp)

En el caso de los generos, se creo un diccionario ordenado que permitiera agregarles pesos, de esta manera fake puede escoger un elemento aleatorio de acuerdo con la probabilidad colocada, agregando tambien el valor de null en 5%, los pesos para masculino y femenino fueron 0.49 y 0.46 respectivmente, esas cantidades se decidieron de manera arbitraria.

Para el estrato socioeconomico tambien se creo un diccionario con pesos, basandome en https://repositorio.cepal.org/server/api/core/bitstreams/3694b22b-988f-4293-abba-ab4bcd4de42a/content los defini como:
* Estrato bajo       52.1%
* Estrato medio      44.5%
* Estrato alto        3.4%

Para la fecha de primera atencion se selecciono una fecha aleatoria utilizando faker, validando que esta fecha no pudiera ser anterior a la fecha de nacimiento del paciente o la fecha en que la empresa inicio operaciones en 1987.

Para la tabla MED_PLANTA el id_med se estalecio de manera consecutiva. En el caso de la especialidad, investigue la categorizacion de especialidades y subespecialidades en https://www.mineducacion.gov.co/1759/articles-403336_Documento_01.pdf para crear un diccionario que permitiera elegir una especialidad al azar y con base en eso una subespecialidad, aunque esta segunda podia quedar nula. Para la fecha de ingreso,calcule la edad del medico para que no pudiera tener menos de 25 años, que es la edad aproximada en la que se empieza a ejercer la medicina, y 75 años, que es una edad de retiro muy elevada, con base en esto elegi una fecha aleatoria y la compare contra la fecha de nacimiento del medico y la fecha de fundacion de la empresa para evitar fechas de ingreso cuando el medico tuviera corta edad o antes de que existiera la empresa. Para el tipo de contrato y jornada investigue los posibles valores.

Para la tabla RED_SEDES el id_sede se tomo consecutivo, el nombre de la sede tomo un nombre de la lista de ciudades aleatorio pero unico, con eso tambien se puede obtener el id_ciudad y el id_pais. el tipo de sede se calculo tomando en cuenta las restricciones de cantidad para cada tipo, se creo una lista que contuviera los distintos tipos por la cantidad de apariciones y se realizo un shuffle para que a cada sede le tocara uno aleatoriamente. Para la capacidad de camas, debido a una confusion de mi parte asumi que la empresaba contaba con 500 mil camas, por lo que realice una matriz de 84x4, cada fila representaba una sede y cada columna un tipo de cama, a continuacion llene la matriz con un ciclo for y numeros aleatorios considerando el numero de camas disponibles y casillas pendientes en cada iteracion buscando garantizar que cada tipo de sede tuviera al menos una cama de cada tipo. El nivel de complejidad se establecio de manera arbitraria, Alta complejidad para hospitales de alta complejidad, Mediana para clinicas de mediana complejidad y Baja para el resto. 

En el caso de la tabla HCE_ENCUENTROS, preferi orientarla principalmente a hospitalizaciones, porque en el primer acercamiento note que incluia campos de fecha de registro y fecha de egreso, por lo que decidi aprovecharla para guardar los registros en los que el paciente tuvo que quedarse en una de las camas, que posteriormente ayudaria a crear la tabla de camas. De esta manera, el id_encuentro quedo consecutivo al igual que los anteriores. Se establecio arbitrariamente que el 27% de los pacientes registrados tuvieran hospitalizacion en su primera atencion, por lo que se creo un ciclo desde 1 hasta esa cantidad





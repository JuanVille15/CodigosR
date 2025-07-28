library(tidyverse)
library(readxl)
library(DBI)
library(odbc)
library(glue)
library(writexl)

# Cargar cédulas
Cedulas <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Estados/Cedulas_Consulta.xlsx")
Cedulas <- Cedulas %>% mutate(ASO_IDENTIFICACION = as.character(ASO_IDENTIFICACION))

# Dividir en bloques de 1000
id_chunks <- split(Cedulas$ASO_IDENTIFICACION, ceiling(seq_along(Cedulas$ASO_IDENTIFICACION) / 1000))

# Conexión
Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "JJf4#rd67J")


# lista para almacenar los resultados
resultados <- list()

# Ejecutar una consulta para cada bloque de IDs

for (ids in id_chunks) {
  id_string <- paste(ids, collapse = ", ") # Convertir el bloque de IDs en una cadena separada por comas
  
  query <- glue("
    SELECT 
f.ASO_IDENTIFICACION,
cli.NOMBRE_COMPLETO,
tel.tel_numero_telefono,
gol.celular as TEL_NUERO_GOLDMEN,
gol.email_1,
gol.direccion_residencia,
cli.tel1,
cli.tel2,
cli.tel3,
cli.cel,
cli.exte
FROM grc.gcc_asociado f
LEFT JOIN (SELECT
            ASO_IDENTIFICACION,
            tel_numero_telefono
            FROM grc.gcc_telefono 
            WHERE tel_tipo_telefono = 'CEL') tel
   ON f.ASO_IDENTIFICACION = tel.ASO_IDENTIFICACION
LEFT  JOIN grc.stg_gcc_golden_ubic_dat gol
   ON f.ASO_IDENTIFICACION = gol.numero_identificacion
LEFT JOIN grc.stg_gcc_sac_infocli cli
   ON f.ASO_IDENTIFICACION = cli.aso_identificacion
  where f.ASO_IDENTIFICACION IN ({id_string});")
  
  
  resultados[[length(resultados) + 1]] <- dbGetQuery(Connecion_SQL, query) # Ejecuta la consulta y almacena el resultado en la lista
}

# Unir todos los resultados
Resultado <- bind_rows(resultados)

write_xlsx(Resultado,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Gestiones_MP.xlsx")

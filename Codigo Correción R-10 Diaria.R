library(dplyr)
library(readxl)
library(DBI)
library(odbc)
library(writexl)
library(glue)

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo/"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

Reporte_10_Diario <- read_excel(ultimo_archivo)

### por la desasignacion de las cedulas, se bebe actualizar la base inicial cada dia
ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Archivos de gestión/Año 2024/9. Septiembre"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

Asignacion <- read_excel(ultimo_archivo)
Asignacion <- select(Asignacion, 1:89)

### para corregir nombres
names(Reporte_10_Diario) <- names(Asignacion)

Cedulas_Faltantes <- anti_join(Asignacion,Reporte_10_Diario, by = c("CEDULA ASOCIADO"="CEDULA ASOCIADO"))

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_13",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "JJ#6g43j")

Base_Cruce <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT STG_GCC_ASOCIADO.ASO_IDENTIFICACION,ASO_FECHA_CAMBIO_ESTADO_MULT,
    ASO_FECHA_INGRESO,
     GCC_ESTADOS_MULTIACTIVA.ESTM_NOMBRE
     FROM GRC.STG_GCC_ASOCIADO
     LEFT JOIN GRC.GCC_ESTADOS_MULTIACTIVA ON STG_GCC_ASOCIADO.ESTM_CODIGO =
     GCC_ESTADOS_MULTIACTIVA.ESTM_CODIGO;"))

dbDisconnect(Connecion_SQL)

Cedulas_Faltantes <- Cedulas_Faltantes %>%
  left_join(Base_Cruce %>% select(ASO_IDENTIFICACION,ESTM_NOMBRE), by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION")) %>% 
  mutate(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` =ESTM_NOMBRE ) %>% 
  select(-ESTM_NOMBRE) %>% 
  filter(!is.na(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`))

Asignacion_dia <- rbind(Reporte_10_Diario, Cedulas_Faltantes) %>%
  distinct(`CEDULA ASOCIADO`,.keep_all = TRUE) %>% 
  filter(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`!=is.na(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`))

Asignacion <- Asignacion %>% select(`CEDULA ASOCIADO`,`ACTOR DE GESTIÓN HOY`,SUBCAMPAÑA,`USUARIO ASIGNADO`)

homologacion <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Homolagacion Asignacion/Homologacion.xlsx")

Asignacion <- Asignacion %>%
  left_join((homologacion), by = c('SUBCAMPAÑA' = 'SUBCAMPAÑA'))

Asignacion_fija <- Asignacion %>% distinct(`CEDULA ASOCIADO`,.keep_all = TRUE)

Asignacion_dia_cruce <- Asignacion_dia %>% 
  left_join((Asignacion_fija),by = c('CEDULA ASOCIADO' = 'CEDULA ASOCIADO'))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  mutate(HOMOLOGACION = ifelse(is.na(HOMOLOGACION),"NO",HOMOLOGACION))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  mutate(`ACTOR DE GESTIÓN HOY.x` = ifelse(HOMOLOGACION == "FIJA",`ACTOR DE GESTIÓN HOY.y`,`ACTOR DE GESTIÓN HOY.x`))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  mutate(SUBCAMPAÑA.x = ifelse(HOMOLOGACION == "FIJA",SUBCAMPAÑA.y,SUBCAMPAÑA.x))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  mutate(`USUARIO ASIGNADO.x` = ifelse(HOMOLOGACION == "FIJA",`USUARIO ASIGNADO.y`,`USUARIO ASIGNADO.x`))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  select(-`ACTOR DE GESTIÓN HOY.y`,-SUBCAMPAÑA.y ,-`USUARIO ASIGNADO.y`,-HOMOLOGACION)

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  rename_with(~ sub("\\.x$", "", .), .cols = ends_with(".x"))

encuentas_contactabilidad <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Archivos de gestión/Ley de intimidad/Autorizaciones_Contactabilidad_Cobranza_Asociados.xlsx")

encuentas_contactabilidad <- transform(
  encuentas_contactabilidad,Identificacion = as.numeric(Identificacion))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>%
  left_join(encuentas_contactabilidad %>% select(Identificacion,`Telefónico (fijo y celular)`), by = c("CEDULA ASOCIADO" = "Identificacion"))

Asignacion_dia_cruce <- Asignacion_dia_cruce %>% rename(`GESTION TELEFONICA`=`Telefónico (fijo y celular)`)

Asignacion_dia_cruce <- Asignacion_dia_cruce %>% 
  mutate(`GESTION TELEFONICA` = ifelse(is.na(`GESTION TELEFONICA`),"SI",`GESTION TELEFONICA`))

v_today <- Sys.Date()

  ## RECORDAR CORREGIR FECHA PARA LA GENERACION EN EL LUGAR CORRECTO
Asignacion_dia_cruce %>%
  write_xlsx(
    glue(
      "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Reporte10_{v_today}_USO INTERNO.xlsx",col_names=TRUE,sheet="Hoja1"))

## GENERACION REGISTROS DEL CSS

sub_css <- c('CSS MORA 2',
             'CSS MORA 3',
             'CSS MORA 4',
             'CSS MORA 5',
             'CSS MORA 6',
             'CSS MORA 7',
             'CSSCUOTA0HASTA6',
             'CSSMORA1CORTE10',
             'CSSMORA1CORTE15',
             'CSSMORA1CORTE20',
             'CSSMORA1CORTE25',
             'CSSMORA1CORTE30',
             'CSSMORA1CORTE5',
             'REG CARIBE',
             'REG CALI',
             'REG BOGOTA',
             'REG MEDELLIN',
             'REG EJECAFETERO',
             'REG PALMIRA')

Asignacion_dia_cruce_css <- Asignacion_dia_cruce %>% 
  filter(SUBCAMPAÑA %in% sub_css,`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)","Solo Asociado") )

### IMPORTANTE CAMBIAR EL MES DE LA CARPETA DONDE SE GENERA LA INFORMACION
Asignacion_dia_cruce_css %>%
  write_xlsx(
    glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2024/9. Septiembre/Reporte10_CSS_{v_today}_USO INTERNO.xlsx",col_names=TRUE))
      
      
## GENERACION y CARGUE DE REGISTROS DE LAS REGIONALES
Asignacion_dia_cruce_Cali <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Cali")

Asignacion_dia_cruce_Bogota <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Bogota")

Asignacion_dia_cruce_Medellin <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Medellin")

Asignacion_dia_cruce_Caribe <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Caribe")

Asignacion_dia_cruce_Eje <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Eje Cafetero")

Asignacion_dia_cruce_Palmira <- Asignacion_dia_cruce %>%
  filter(REGIONAL=="Palmira")

Asignacion_dia_cruce_Cali %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Cali - Reporte 10/R10 Corregido SCP/Reporte10_Cali_USO INTERNO.xlsx",col_names=TRUE))

Asignacion_dia_cruce_Bogota %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Bogota - Reporte 10/R10 Corregido SCP/Reporte10_Bogota_USO INTERNO.xlsx",col_names=TRUE))

Asignacion_dia_cruce_Medellin %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Medellin - Reporte 10/R10 Corregido SCP/Reporte10_Medellin_USO INTERNO.xlsx",col_names=TRUE))

Asignacion_dia_cruce_Caribe %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Caribe - Reporte 10/R10 Corregido SCP/Reporte10_Caribe_USO INTERNO.xlsx",col_names=TRUE))

Asignacion_dia_cruce_Eje %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Eje Cafetero - Reporte 10/R10 Corregido SCP/Reporte10_Eje_Cafetero_USO INTERNO.xlsx",col_names=TRUE))

Asignacion_dia_cruce_Palmira %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/Informe Regionales/",
      "Regional Palmira - Reporte 10/R10 Corregido SCP/Reporte10_Palmira_USO INTERNO.xlsx",col_names=TRUE))


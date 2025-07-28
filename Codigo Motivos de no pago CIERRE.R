# Libraries ---------------------------------------------------------------

library(conflicted)
library(dplyr)
library(dbplyr)
library(lubridate)
library(glue)
library(odbc)
library(janitor)
library(readr)
library(Microsoft365R)
library(blastula)
library(readxl)
library(janitor)
library(writexl)

conflict_prefer("filter", "dplyr")

# Parameters --------------------------------------------------------------

# v_end <- ceiling_date(today() %m-% months(1), unit = "month") %m-% days(1)

v_end <- ceiling_date(ymd(20250615), unit = "month") %m-% days(1)

v_start <- floor_date(v_end, unit = "month")

priorizacion <- read_xlsx("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Motivos de No Pago/priorizacion_motivos_de_no_pago.xlsx",
  col_types = "text",
  .name_repair = ~ make_clean_names(., "all_caps")
)

# Import data -------------------------------------------------------------

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KyCo_090625*")

# La tabla original solo guarda 12 meses de gestiones, para consultas 
# superiores a un año de antiguedad se debe cambiar a la tabla "historia_gestion_hstr
# (no contiene el ultimo año)"

data_local_sac_rnp <- tbl(Connecion_SQL, in_schema("GRC", "GCC_HISTORIA_GESTIONES")) %>%
  filter(
    GES_FECHA_GESTION >= TO_DATE(v_start, 'YYYY-MM-DD'),
    GES_FECHA_GESTION <= TO_DATE(v_end, 'YYYY-MM-DD'),
    MOTV_NOMBRE_MOTIVO != 'NA'
  ) %>%
  select(
    ASO_IDENTIFICACION,
    GES_FECHA_GESTION,
    CAS_NOMBRE_CASA,
    AUX_USUARIO,
    ACC_NOMBRE_ACCION,
    RES_NOMBRE_RESPUESTA,
    HGES_CONTACTO,
    HGES_OBSERVACION,
    MOTV_NOMBRE_MOTIVO
  ) %>%
  collect() %>%
  clean_names(case = "all_caps") %>%
  left_join(priorizacion, by = "MOTV_NOMBRE_MOTIVO") %>%
  arrange(PRIORIDAD, desc(GES_FECHA_GESTION)) %>% 
  distinct(ASO_IDENTIFICACION, .keep_all = TRUE)

dbDisconnect(Connecion_SQL)

### Cargue reporte 10 de cierre SE DEBE CAMBAIR LA RUTA CADA MES

ruta_carpeta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
reporte_10_cierre <- read_excel(ultimo_archivo) %>% 
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)

## Realizar los cruces ##

data_local_sac_rnp <- data_local_sac_rnp %>% left_join(reporte_10_cierre %>% select(CEDULA_ASOCIADO,REGIONAL,ZONA,EDAD_CARTERA_INICIAL,EDAD_CARTERA_HOY,SUBCAMPANA,DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY), by = c("ASO_IDENTIFICACION" = "CEDULA_ASOCIADO"))


data_local_sac_rnp <- data_local_sac_rnp %>% rename(ESTADO_CIERRE = DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY)


## Se trae el Perfil del asociado ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Perfil Asociado"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Perfil <- read_excel(ultimo_archivo)

## Cruzamos el perfil ##

data_local_sac_rnp <- data_local_sac_rnp %>% left_join(Perfil %>% select(strIdentificacion,strAgrupacionPerfil), by = c("ASO_IDENTIFICACION" = "strIdentificacion"))

data_local_sac_rnp <- data_local_sac_rnp %>% rename(Perfil_Asociado = strAgrupacionPerfil)


data_local_sac_rnp %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Motivos de No Pago/Resultado",
      "motivos_no_pago_SAC_{v_start}_{v_end}_USO-INTERNO.xlsx"
    )
  )

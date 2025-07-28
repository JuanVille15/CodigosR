library(dplyr)
library(readxl)
library(writexl)
library(lubridate)
library(glue)
library(tools)
library(janitor)
library(odbc)
library(dbplyr)
library(tidyverse)

## Leemos Archivo de Retenciones Efectivas ##

ruta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Retención/7. Julio"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)


## Empezamos a crear un Data Frame a partir de las Efectivas ##

Base_cargue <- data %>% select(`Número de Identificación`, `Fecha de creación`)

Base_cargue <- Base_cargue %>% mutate(TipoGestion = 1,
                                      CasaDeCobro = "DIR NAC RECAUDO",
                                      Usuario = "MAAC1036",
                                      Gestion = "LLAMADA RETENC TELEF",
                                      Respuesta = "TRASLADO_A_RETENCION",
                                      Contacto = "ASOCIADO")

Base_cargue <- Base_cargue %>% select(TipoGestion, `Número de Identificación`, CasaDeCobro, Usuario, `Fecha de creación`, Gestion, Respuesta, Contacto)

data <- data %>% mutate(Observacion = paste("Resultado de la Gestión:", `Validacion con Estado`, "Principal Motivo de Retiro:", `Principal Motivo de Retiro`, 
                                            "Observaciones:", Observaciones, "Anillo 1 Oferta 1 : Herramienta:", `Anillo 1 Oferta 1 : Herramienta`,
                                            "Anillo 1 Oferta 2 : Herramienta:", `Anillo 1 Oferta 2 : Herramienta`, "Anillo 2 Oferta 1 : Herramienta:", `Anillo 2 Oferta 1 : Herramienta`,
                                            "Anillo 2 Oferta 2 : Herramienta:", `Anillo 2 Oferta 2 : Herramienta`, "Anillo 3 Oferta 1 : Herramienta:", `Anillo 3 Oferta 1 : Herramienta`) )

Base_cargue <- Base_cargue %>% left_join(data %>% select(`Número de Identificación`, Observacion), by = "Número de Identificación")



## Consultamos el telefono del asociado en GCC ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KysCoL_140725*")


Telefonos <- dbGetQuery(Connecion_SQL, glue_sql("SELECT 
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
                                                          ON f.ASO_IDENTIFICACION = cli.aso_identificacion;"))


Base_cargue <- Base_cargue %>% left_join(Telefonos %>% select(ASO_IDENTIFICACION, TEL_NUERO_GOLDMEN), by = c("Número de Identificación" = "ASO_IDENTIFICACION"))

Base_cargue <- Base_cargue %>% rename(Identificacion = `Número de Identificación`,
                                      FechaGestion = `Fecha de creación`,
                                      Telefono = TEL_NUERO_GOLDMEN)

Base_cargue <- Base_cargue %>% mutate(MotivoDeNoPago = "",
                                      FechaProxGestion = "",
                                      TiempoGestion = "",
                                      NumeroCuenta = "",
                                      ValorPromesa = "",
                                      fechaPromesa = "",
                                      DireccionVisita = "",
                                      SubCampaña = "",
                                      Canal = "")

Base_cargue <- Base_cargue %>% mutate(FechaGestion = format(as.Date(FechaGestion), "%d-%m-%Y"))

Base_cargue <- Base_cargue %>% mutate(across(everything(), as.character))

write_xlsx(Base_cargue,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Retención/7. Julio/Resultado/Base_Cargue11072025.xlsx")                                 

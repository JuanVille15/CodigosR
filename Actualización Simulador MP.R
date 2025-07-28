### Script para generar la base con la cual se gestiona el alivio Bita MP.

library(readxl)
library(dplyr)
library(readxl)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(tidyr)
library(lubridate)
library(stringr)
library(openxlsx)

## CREACIÓN BD INSUMO SIMULADOR ##

Ruta_inicial <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/MP/Simulador"
archivos_con_fechas <- file.info(list.files(path = Ruta_inicial, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
base_inicial <- read_excel(ultimo_archivo)%>%
    clean_names(case = "all_caps") %>%
      filter(APLICAR_ALIVIO == "Si_Aplica")




BD_transformada <- base_inicial %>%  select(LLAVE, NIT, CO_SUBPRODUCTO, CO_TOPERACION, 
                                                    CONTRATO, CO_SALDO, CO_MORA_XPAGAR, CO_CUOTAS_VENCIDAS) %>%
                                                       mutate(USUARIOS = NA) %>%  
                                                       bind_cols(base_inicial %>% 
                                          select(CO_NOMBRE_OFI, REGIONAL2, CO_DIA_CORTE, PERFIL, 
                                                 SINESTRALIDAD, CONGELAMIENTO_60_MI, 
                                                 QCSDES, DIREC, QCSDIE, QCSTE1, QCSTE2, QCSTE3, QCSTE4 ,QCSTE5, QCSTE6))

BD_transformada <- BD_transformada %>% mutate(PROGRAMA = CO_SUBPRODUCTO,
                                              SEG_SINIESTRA = NA,    
                                              EDAD = NA,    
                                              SEG_EDAD = NA,    
                                              ANTIGÜEDAD = NA,    
                                              SEG_ANTIGUE_DAV = NA,    
                                              NPS = NA,    
                                              CATEGORIA = NA,    
                                              Alivio_Otorgado_Perrsuasión = NA,    
                                              Alivio_Otorgado_Cobranza = NA,
                                              Densidad_Contrato = NA)

BD_transformada <- BD_transformada %>%  rename(`CONGELAMIENTO >60 MI` = CONGELAMIENTO_60_MI)

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio MP/Base Alivio Mp"
nombre_archivo <- paste0(ruta, "/Alivio_Plan_Asociado_MP_", format(Sys.Date(), "%B"), ".xlsx")
write.xlsx(BD_transformada, file = nombre_archivo, rowNames = FALSE)


## CREACIÓN BD SIMULADOR ##

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio MP/Base Alivio Mp"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

base_bita <- read_excel(ultimo_archivo) %>% 
  clean_names(case = "all_caps") %>% 
  transform(NIT=as.numeric(NIT),
            CONTRATO=as.character(CONTRATO)) 


Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_13",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "Jju3v#A5t93")

BASE_FACTURA <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
fac.GCC_TIPO_ALFABETICO,
fac.ASO_IDENTIFICACION,
fac.CON_CODIGO,
fac.FACT_INTERES_MORA_DIA,
fac.FACT_VALOR_CUOTA_MES,
fac.FACT_VALOR_VENCIDO,
fac.FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS TOTAL,
fac.FACT_NUMERO_CUOTAS_ATRASADAS,
fac.FACT_NUMERO_PAGARE,
fac.COR_NUMERO_CORTE,
agr.AGR_DESCRIPCION,
axe.age_nombre
from grc.stg_gcc_facturacion fac
LEFT OUTER JOIN grc.gcc_concepto con
   ON fac.con_codigo = con.con_codigo
LEFT OUTER JOIN grc.gcc_agrupacion agr
   ON con.agr_codigo = agr.agr_codigo
LEFT OUTER JOIN grc.gcc_agrupacion_empresa axe
   ON agr.age_codigo = axe.age_codigo
WHERE axe.age_nombre = 'MEDICINA PREPAGADA'
AND fac.CON_CODIGO NOT IN ('CEMR',
'01CEAB',
'01CEAD',
'01CEAG',
'01CEAM',
'01CEAP',
'01CEAS',
'01CEAT',
'01CEA9',
'01CEB',
'01CEB1',
'01CECA',
'01CECE',
'01CEC1',
'01CEEF',
'01CEE1',
'01CEE2',
'01CEFE',
'01CEFG',
'01CEFI',
'01CEFO',
'01CEGF',
'01CEIN',
'01CEM',
'01CEMA',
'01CEMB',
'01CEMC',
'01CEME',
'01CEMH',
'01CEMJ',
'01CEMO',
'01CEMS',
'01CEMX',
'01CEMZ',
'01CEM1',
'01CEM3',
'01CEM4',
'01CEM7',
'01CEM8',
'01CEOC',
'01CEPD',
'01CEPM',
'01CEPO',
'01CEPR',
'01CEPS',
'01CEP2',
'01CERP',
'01CETE',
'01CET9',
'01CEUS',
'01CEX0',
'01CEX1',
'01CEX3',
'01CE19',
'01CE2P',
'01CE23',
'01CPEM',
'01EVEM',
'04CEAB',
'04CEAD',
'04CEAG',
'04CEAM',
'04CEAP',
'04CEAS',
'04CEAT',
'04CEA9',
'04CEB',
'04CEB1',
'04CECA',
'04CECE',
'04CEC1',
'04CEEF',
'04CEE1',
'04CEE2',
'04CEFE',
'04CEFG',
'04CEFI',
'04CEFO',
'04CEGF',
'04CEIN',
'04CEM',
'04CEMA',
'04CEMB',
'04CEMC',
'04CEME',
'04CEMH',
'04CEMJ',
'04CEMO',
'04CEMS',
'04CEMX',
'04CEMZ',
'04CEM1',
'04CEM3',
'04CEM4',
'04CEM7',
'04CEM8',
'04CEOC',
'04CEPD',
'04CEPM',
'04CEPO',
'04CEPR',
'04CEPS',
'04CEP2',
'04CERP',
'04CETE',
'04CET9',
'04CEUS',
'04CEX0',
'04CEX1',
'04CEX3',
'04CE19',
'04CE2P',
'04CE23',
'04CPEM',
'04EVEM',
'07CEM',
'07CEMC',
'07CEMO',
'07CEMS',
'07CEMZ',
'07CEM1',
'07CEM3',
'07CEM4',
'07CEM7',
'07CEM8',
'08CEM',
'08CEMC',
'08CEMJ',
'08CEMO',
'08CEMS',
'08CEMZ',
'08CEM1',
'08CEM3',
'08CEM4',
'08CEM7',
'08CEM8',
'09CEAG',
'09CEAP',
'09CEAS',
'09CEAT',
'09CEFE',
'09CEFG',
'09CEFO',
'09CEGF',
'09CEM',
'09CEMC',
'09CEMH',
'09CEMO',
'09CEMZ',
'09CEM1',
'09CEM3',
'09CEM4',
'09CEM7',
'09CEM8',
'09CEPD',
'09CEPM',
'09CEPO',
'09CEPR',
'09CEPS',
'09CEP2',
'09CERP',
'09CEX3',
'09CE2P',
'01CEMQ',
'04CEMQ',
'01CE50',
'04CE50',
'01PACE',
'04PACE',
'01TACE',
'04TACE',
'01CUE3',
'04CUE3',
'01CUE6',
'04CUE6',
'01CUE9',
'04CUE9',
'01CU12',
'04CU12',
'01CU23',
'04CU23',
'01CU26',
'04CU26',
'01CU29',
'04CU29',
'01CU22',
'04CU22',
'01CTL3',
'04CTL3',
'01CTL6',
'04CTL6',
'01CTL9',
'04CTL9',
'01CT12',
'04CT12',
'01CT23',
'04CT23',
'01CT26',
'04CT26',
'01CT29',
'04CT29',
'01CT22',
'04CT22');"))

dbDisconnect(Connecion_SQL)

BASE_FACTURA <- BASE_FACTURA %>%
  semi_join(base_bita, by=c("ASO_IDENTIFICACION"="NIT")) %>% 
  transform(FACT_NUMERO_PAGARE=as.character(FACT_NUMERO_PAGARE))

# Función para verificar la presencia del número de contrato en el número de pagaré
extraer_contrato <- function(FACT_NUMERO_PAGARE, ASO_IDENTIFICACION, CONTRATOS) {
  CONTRATOS <- CONTRATOS[base_bita$NIT == ASO_IDENTIFICACION]  # Filtrar contratos por número de documento
  for (CONTRATO in CONTRATOS) {
    if (str_detect(FACT_NUMERO_PAGARE, CONTRATO)) {
      return(CONTRATO)
    }
  }
  return(NA)
}

# Aplicar la función a dfB dentro de cada grupo por número de documento
BASE_FACTURA <- BASE_FACTURA %>%
  group_by(ASO_IDENTIFICACION) %>%
  mutate(NUMERO_CONTRATO = sapply(FACT_NUMERO_PAGARE, extraer_contrato, ASO_IDENTIFICACION = ASO_IDENTIFICACION, CONTRATOS = base_bita$CONTRATO)) %>%
  ungroup()

# Filtrar filas que tienen un número de contrato extraído
BASE_FACTURA_filtrado <- BASE_FACTURA %>%
  filter(!is.na(NUMERO_CONTRATO))

# Sumar los valores facturados en BASE_FACTURA agrupados por el número de contrato y número de documento
BASE_FACTURA_sum <- BASE_FACTURA_filtrado %>%
  group_by(NUMERO_CONTRATO, ASO_IDENTIFICACION) %>%
  summarise(SUMA_VENCIDO_CONTRATO = sum(FACT_VALOR_VENCIDO))

# Unir base_bita con BASE_FACTURA_sum usando el número de contrato y número de documento
base_bita <- base_bita %>%
  left_join(BASE_FACTURA_sum, by = c("CONTRATO"="NUMERO_CONTRATO")) %>% 
  transform(CONTRATO=as.numeric(CONTRATO)) %>% 
  select(-ASO_IDENTIFICACION)

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Archivos de gestión/Año 2024/10. Octubre"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Asignacion <- read_excel(ultimo_archivo)
Asignacion <- select(Asignacion, 1:89) %>% 
  clean_names(case = "all_caps")

base_bita <- base_bita %>%
  left_join(Asignacion %>% select(CEDULA_ASOCIADO,
                                  SUBCAMPANA), by = c("NIT"="CEDULA_ASOCIADO"))

write_xlsx(base_bita,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio MP/Resultado/Base_ofrecimiento.xlsx")

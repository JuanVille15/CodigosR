### Script para generar la base con la cual se gestiona la Anulacion ACR.

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

### Primero: se debe definir cual es el reporte 10 con la asignacion inicial

Asignacion_Inicial <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo/Reporte10_2025-07-02.xlsx")
Asignacion_Inicial <- Asignacion_Inicial %>% 
  clean_names(case = "all_caps")%>% 
  filter(EDAD_CARTERA_INICIAL %in% c(2,3,4),
         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA >= 1,
         NO_CUOTAS_CANCELADAS >= 12,
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY=="Activo Cobranza Interna",
         SUBCAMPANA %in% c("CSS MORA 2","CSS MORA 3","CSS MORA 4","REG CALI","REG CARIBE","REG EJECAFETERO","REG MEDELLIN","REG BOGOTA","REG PALMIRA"),
         TIPO_CLIENTE %in% c("Mixto (Asociado/Cliente)","Solo Asociado")) %>% 
  select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,EDAD_CARTERA_INICIAL,SUBCAMPANA,DESCRIPCION_DEL_CORTE) %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)

### Cargue Base Nuevos Inactivos

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/00. Comité Interno Recaudo/2. Reportes de Cierre/Reportes Cierre_2025/1. Activos/1.2. Nuevos inactivos"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

Nuevos_Inactivos <- read_excel(ultimo_archivo,sheet = "DATA_NVS_INACTIVOS")

### rango de tiempo a considerar para descartar por inactividad
fecha_actual <- Sys.Date()- 365
periodo_considerar <-floor_date(fecha_actual, unit = "month")

### Bases para descartar por estrategias y/o inactividad
Nuevos_Inactivos <- Nuevos_Inactivos %>% 
  clean_names(case = "all_caps") %>% 
  filter(MES_INACTIVIDAD>= {periodo_considerar}) %>% 
  distinct(CEDULA) %>% 
  mutate(APLICA_INAC="SI")

Alivio_SP <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Proceso Inactivos/Recursos con destinación especifica para Reactivación/Control_Aplicaciones_GCCA_SP.xlsx",sheet = "CONSOLIDADO APLICACIONES")
Alivio_SP <- Alivio_SP %>% 
  clean_names(case = "all_caps")%>% 
  filter(FECHA_ENVIO_OPERACIONES!=is.na(FECHA_ENVIO_OPERACIONES)) %>%
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>%
  distinct(CEDULA_ASOCIADO)%>% 
  mutate(APLICA_SP="SI")

ANULACION_ACR <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/Consolidado Anulacion ACR.xlsx",sheet = "Consolidado")
ANULACION_ACR <- ANULACION_ACR %>% 
  clean_names(case = "all_caps") %>%
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>%
  distinct(CEDULA)%>%
  mutate(APLICA_ACR="SI")

ALIVIO_FINANCIERO <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Recaudo MT/Alivio Financiero/Seguimiento Alivio Financiero V2.xlsx", sheet = "Table1")
ALIVIO_FINANCIERO <- ALIVIO_FINANCIERO %>% 
  clean_names(case = "all_caps") %>% 
  filter(ESTADO == "APLICADO") %>%
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>%
  distinct(IDENTIFICACION_ASOCIADO)%>%
  mutate(APLICA_FINAN="SI")

PERSEVERADOS <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/Insumo Simulador/Reporte_Asociados_Perseverados.xlsx")
PERSEVERADOS <- PERSEVERADOS %>% 
  clean_names(case = "all_caps") %>% 
  distinct(IDENTIFICACION)%>% 
  mutate(APLICA_PER="SI")

###Cruce con las bases de los alivios y la poblacion que aplica


Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(Alivio_SP, by = c("CEDULA_ASOCIADO"= "CEDULA_ASOCIADO")) %>% 
  mutate(APLICA_SP = ifelse(is.na(APLICA_SP),"NO",APLICA_SP))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(ALIVIO_FINANCIERO, by = c("CEDULA_ASOCIADO"= "IDENTIFICACION_ASOCIADO")) %>% 
  mutate(APLICA_FINAN = ifelse(is.na(APLICA_FINAN),"NO",APLICA_FINAN))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(PERSEVERADOS, by = c("CEDULA_ASOCIADO"= "IDENTIFICACION")) %>% 
  mutate(APLICA_PER = ifelse(is.na(APLICA_PER),"NO",APLICA_PER))

Asignacion_Inicial <- Asignacion_Inicial %>% mutate(CEDULA_ASOCIADO = as.double(CEDULA_ASOCIADO))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(ANULACION_ACR, by = c("CEDULA_ASOCIADO"= "CEDULA")) %>% 
  mutate(APLICA_ACR = ifelse(is.na(APLICA_ACR),"NO",APLICA_ACR))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(Nuevos_Inactivos, by = c("CEDULA_ASOCIADO"= "CEDULA")) %>% 
  mutate(APLICA_INAC = ifelse(is.na(APLICA_INAC),"NO",APLICA_INAC))

Asignacion_Inicial <- Asignacion_Inicial %>%
  filter(APLICA_SP=="NO",APLICA_FINAN=="NO",APLICA_PER=="NO",APLICA_ACR=="NO",APLICA_INAC=="NO") %>% 
  select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,EDAD_CARTERA_INICIAL,SUBCAMPANA,DESCRIPCION_DEL_CORTE)

####Descarga de facturacion actualizada para construccion del simulador

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KyCo_090625*")

BASE_FACTURA <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT
    fac.ASO_IDENTIFICACION,
    fac.CON_CODIGO,
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
   ON agr.age_codigo = axe.age_codigo;"))

dbDisconnect(Connecion_SQL)

### Organizar la facturacion para incluir en el simulador

CONDONA <- BASE_FACTURA %>% 
  semi_join(Asignacion_Inicial, by = c("ASO_IDENTIFICACION"="CEDULA_ASOCIADO")) %>% 
  select(ASO_IDENTIFICACION,CON_CODIGO,FACT_VALOR_VENCIDO) %>% 
  filter(CON_CODIGO %in% c("APORTES","CALAMID","RECREA"))

CONDONA <- CONDONA %>%
  pivot_wider(names_from = CON_CODIGO, values_from = FACT_VALOR_VENCIDO)
CONDONA[is.na(CONDONA)] <- 0

PAGA <- BASE_FACTURA %>%
  semi_join(Asignacion_Inicial, by = c("ASO_IDENTIFICACION"="CEDULA_ASOCIADO")) %>%
  filter(!(CON_CODIGO %in% c("APORTES","CALAMID","RECREA")),
         AGE_NOMBRE!= "BANCOOMEVA") %>% 
  select(ASO_IDENTIFICACION,FACT_VALOR_VENCIDO) %>% 
  group_by(ASO_IDENTIFICACION) %>% 
  summarize(VALOR_A_PAGAR_ASOCIADO= sum(FACT_VALOR_VENCIDO))

COMPLETO <- CONDONA %>% 
  left_join(PAGA, by=c("ASO_IDENTIFICACION"="ASO_IDENTIFICACION"))
COMPLETO[is.na(COMPLETO)] <- 0

COMPLETO <- COMPLETO %>% 
  mutate(FACTURACION_TOTAL=APORTES+CALAMID+RECREA+VALOR_A_PAGAR_ASOCIADO)

COMPLETO <- COMPLETO %>% 
  mutate(PORCENTAJE_DE_CONDONACION=(APORTES+CALAMID+RECREA)/FACTURACION_TOTAL) %>% 
  filter(PORCENTAJE_DE_CONDONACION>=0.1,
         PORCENTAJE_DE_CONDONACION<=0.6)

COMPLETO <- COMPLETO %>%
  mutate(TOTAL_A_CONDONAR=APORTES+CALAMID+RECREA)

COMPLETO <- COMPLETO %>%
  left_join(Asignacion_Inicial,by=c("ASO_IDENTIFICACION"="CEDULA_ASOCIADO"))

###Base definitiva

COMPLETO <- COMPLETO %>%
  select(ASO_IDENTIFICACION,
         NOMBRE_ASOCIADO,
         EDAD_CARTERA_INICIAL,
         SUBCAMPANA,
         DESCRIPCION_DEL_CORTE,
         APORTES,
         CALAMID,
         RECREA,
         VALOR_A_PAGAR_ASOCIADO,
         FACTURACION_TOTAL,
         PORCENTAJE_DE_CONDONACION,
         TOTAL_A_CONDONAR)


write_xlsx(COMPLETO,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Anulacion ACR/Resultado/Base_ofrecimiento.xlsx")

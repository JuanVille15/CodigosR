### Script para generar la base con la cual se gestiona el alivio financiero mes a mes.

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

conflicts_prefer(dplyr::filter)

### Primero: se debe definir cual es el reporte 10 con la asignacion inicial

Asignacion_Inicial <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo/Reporte10_2025-06-05.xlsx")
Asignacion_Inicial <- Asignacion_Inicial %>% 
  clean_names(case = "all_caps")%>% 
  filter(EDAD_CARTERA_HOY == 4,
         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA > 2,
         NO_CUOTAS_CANCELADAS >= 24,
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY=="Activo Cobranza Interna",
         TIPO_CLIENTE %in% c("Mixto (Asociado/Cliente)","Solo Asociado")) %>% 
  select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,EDAD_CARTERA_INICIAL,SUBCAMPANA)


### Segundo: debemos cargar las bases de los alivios y perseverados

fecha_actual <- Sys.Date()- 365
periodo_considerar <-floor_date(fecha_actual, unit = "month")

Alivio_SP <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Proceso Inactivos/Recursos con destinación especifica para Reactivación/Control_Aplicaciones_GCCA_SP.xlsx",sheet = "CONSOLIDADO APLICACIONES")
Alivio_SP <- Alivio_SP %>% 
  clean_names(case = "all_caps")%>% 
  filter(FECHA_ENVIO_OPERACIONES!=is.na(FECHA_ENVIO_OPERACIONES)) %>%
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>% 
  distinct(CEDULA_ASOCIADO)%>% 
  mutate(APLICA_SP="SI")

ANULACION_ACR <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/Consolidado Anulacion ACR.xlsx", sheet = "Consolidado")
ANULACION_ACR <- ANULACION_ACR %>% 
  clean_names(case = "all_caps") %>%
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>%
  distinct(CEDULA) %>% 
  mutate(APLICA_ACR="SI")

ALIVIO_FINANCIERO <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Recaudo MT/Alivio Financiero/Seguimiento Alivio Financiero V2.xlsx", sheet = "Table1")
ALIVIO_FINANCIERO <- ALIVIO_FINANCIERO %>% 
  clean_names(case = "all_caps") %>% 
  filter(ESTADO == "APLICADO" | ESTADO == "EN PROCESO" ) %>% 
  filter(FECHA_ENVIO_OPERACIONES>={periodo_considerar}) %>%
  distinct(IDENTIFICACION_ASOCIADO) %>% 
  mutate(APLICA_FINAN="SI")

PERSEVERADOS <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/Insumo Simulador/Reporte_Asociados_Perseverados.xlsx")
PERSEVERADOS <- PERSEVERADOS %>% 
  clean_names(case = "all_caps") %>%
  distinct(IDENTIFICACION)%>% 
  mutate(APLICA_PER="SI")

### Tercero: Cruce con las bases de los alivios y la poblacion que aplica


Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(Alivio_SP, by = c("CEDULA_ASOCIADO"= "CEDULA_ASOCIADO")) %>% 
  mutate(APLICA_SP = ifelse(is.na(APLICA_SP),"NO",APLICA_SP))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(ALIVIO_FINANCIERO, by = c("CEDULA_ASOCIADO"= "IDENTIFICACION_ASOCIADO")) %>% 
  mutate(APLICA_FINAN = ifelse(is.na(APLICA_FINAN),"NO",APLICA_FINAN))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(PERSEVERADOS, by = c("CEDULA_ASOCIADO"= "IDENTIFICACION")) %>% 
  mutate(APLICA_PER = ifelse(is.na(APLICA_PER),"NO",APLICA_PER))

Asignacion_Inicial <- Asignacion_Inicial %>% 
  left_join(ANULACION_ACR, by = c("CEDULA_ASOCIADO"= "CEDULA")) %>% 
  mutate(APLICA_ACR = ifelse(is.na(APLICA_ACR),"NO",APLICA_ACR))

Asignacion_Inicial <- Asignacion_Inicial %>%
  filter(APLICA_SP=="NO",APLICA_FINAN=="NO",APLICA_PER=="NO",APLICA_ACR=="NO") %>% 
  select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,EDAD_CARTERA_INICIAL,SUBCAMPANA)

### Cuarto: Extraemos la facturacion para actualizar el simulador

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "TyhU_RtQ3491*")

BASE_FACTURA <- dbGetQuery(Connecion_SQL,glue_sql(
  "Select GCC_TIPO_ALFABETICO,
    ASO_IDENTIFICACION,
    CON_CODIGO,
    FACT_INTERES_MORA_DIA,
    FACT_VALOR_CUOTA_MES,
    FACT_VALOR_VENCIDO,
    FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS TOTAL,
    FACT_NUMERO_CUOTAS_ATRASADAS,
    FACT_NUMERO_PAGARE,
    COR_NUMERO_CORTE
    from GRC.STG_GCC_FACTURACION
    where CON_CODIGO in ('AUX.FUN.','SOLIDAR.','SOLIDARE','APORTES','CALAMID','RECREA');"))

dbDisconnect(Connecion_SQL)

BASE_FACTURA <- BASE_FACTURA %>%
  semi_join(Asignacion_Inicial,by = c("ASO_IDENTIFICACION"="CEDULA_ASOCIADO")) %>% 
  select(ASO_IDENTIFICACION,CON_CODIGO,FACT_VALOR_VENCIDO)

### Quinto: Organizamos los datos para que se puedan cargar facilmente en el simulador

BASE_FACTURA <- BASE_FACTURA %>%
  pivot_wider(names_from = CON_CODIGO, values_from = FACT_VALOR_VENCIDO) 
BASE_FACTURA[is.na(BASE_FACTURA)] <- 0

BASE_FACTURA <- BASE_FACTURA %>%
  left_join(Asignacion_Inicial,by = c("ASO_IDENTIFICACION"="CEDULA_ASOCIADO"))

BASE_FACTURA <- BASE_FACTURA %>% 
  mutate(EDAD_CARTERA_INICIAL= ifelse(EDAD_CARTERA_INICIAL==3,0.5,0.4)) %>% 
  rename(PORCENTAJE_CONDONACION=EDAD_CARTERA_INICIAL)

BASE_FACTURA <- BASE_FACTURA %>%
  mutate(TOTAL_ADEUDADO=AUX.FUN.+SOLIDAR.+SOLIDARE)

BASE_FACTURA <- BASE_FACTURA %>%
  mutate(TOTAL_A_PAGAR_POR_COOMEVA=ifelse((TOTAL_ADEUDADO*PORCENTAJE_CONDONACION)>750000,750000,TOTAL_ADEUDADO*PORCENTAJE_CONDONACION))

BASE_FACTURA <- BASE_FACTURA %>%
  mutate(TOTAL_A_PAGAR_POR_ASOCIADO=TOTAL_ADEUDADO-TOTAL_A_PAGAR_POR_COOMEVA)

BASE_FACTURA <- BASE_FACTURA %>%
  mutate(TOTAL_A_CONDONAR=APORTES+CALAMID+RECREA)

BASE_ALIVIO <- BASE_FACTURA %>%
  filter(TOTAL_A_PAGAR_POR_ASOCIADO!=0)

BASE_ALIVIO <- BASE_ALIVIO %>%
  select(ASO_IDENTIFICACION,NOMBRE_ASOCIADO,TOTAL_A_PAGAR_POR_COOMEVA,PORCENTAJE_CONDONACION,AUX.FUN.,SOLIDAR.,SOLIDARE,TOTAL_ADEUDADO,TOTAL_A_PAGAR_POR_ASOCIADO,APORTES,CALAMID,RECREA,TOTAL_A_CONDONAR,SUBCAMPANA)

write_xlsx(BASE_ALIVIO,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/Insumo Simulador/Resultado/BASE_ALIVIO.xlsx")

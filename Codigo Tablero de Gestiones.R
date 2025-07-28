### tablero de gestion

library(readxl)
library(dplyr)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(tidyr)
library(lubridate)
library(conflicted)
library(dbplyr)
library(readr)
library(Microsoft365R)
library(blastula)

conflict_prefer("filter", "dplyr")

base_gestiones <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Gestiones Provisionales/Gestiones_Mensuales.xlsx") %>%
  clean_names(case = "all_caps") %>% 
  transform(IDENTIFICACION_DEUDOR=as.numeric(IDENTIFICACION_DEUDOR))

### Cargue Base MP

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Base MP/2025"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
mp <- read_excel(ultimo_archivo, sheet = "BD") %>%
  clean_names(case = "all_caps") %>% 
  distinct(NIT) %>% 
  mutate(MP="SI")

### Cargue Base CEM

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Base CEM/2025"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
cem <- read_excel(ultimo_archivo) %>%
  clean_names(case = "all_caps") %>% 
  distinct(CC_ASOCIADO) %>% 
  mutate(CEM="SI")


### TENER PRESENTE ACTUALIZAR LA CARPETA CORRECTA## R-10

ruta_carpeta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
asignacion_dia <- read_excel(ultimo_archivo) %>%
  clean_names(case = "all_caps")

### Subcampañas a considerar para la construccion del tablero

sub_css <- c('CSS MORA 2',
             'CSS MORA 3',
             'CSS MORA 4',
             'CSS MORA 5',
             'CSS MORA 6',
             'CSS MORA 7',
             'CSSMORA1CORTE10',
             'CSSMORA1CORTE15',
             'CSSMORA1CORTE20',
             'CSSMORA1CORTE25',
             'CSSMORA1CORTE30',
             'CSSMORA1CORTE5',
             'REG BOGOTA',
             'REG CARIBE',
             'REG EJECAFETERO',
             'REG MEDELLIN',
             'REG PALMIRA',
             'REG CALI',
             'CSSCUOTA0HASTA6')


asignacion_dia <- asignacion_dia %>%
  filter(SUBCAMPANA %in% sub_css,
         TIPO_CLIENTE %in% c("Mixto (Asociado/Cliente)","Solo Asociado"),
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY %in% c("Activo Normal","Activo Cobranza Interna","Inactivo","Receso")) %>%
  distinct(CEDULA_ASOCIADO, .keep_all = TRUE) %>% 
  mutate(EDAD_CARTERA_INICIAL=ifelse(is.na(EDAD_CARTERA_INICIAL),0,EDAD_CARTERA_INICIAL),
         EDAD_CARTERA_HOY=ifelse(is.na(EDAD_CARTERA_HOY),0,EDAD_CARTERA_HOY),
         USUARIO_ASIGNADO=ifelse(is.na(USUARIO_ASIGNADO),"SIN USUARIO",USUARIO_ASIGNADO),
         EDAD_CARTERA_INICIAL=ifelse(EDAD_CARTERA_INICIAL>4,4,EDAD_CARTERA_INICIAL),
         EDAD_CARTERA_HOY=ifelse(EDAD_CARTERA_HOY>4,4,EDAD_CARTERA_HOY)) %>% 
  select(CEDULA_ASOCIADO,
         ACTOR_DE_GESTION_HOY,
         SUBCAMPANA,
         USUARIO_ASIGNADO,
         EDAD_CARTERA_INICIAL,
         EDAD_CARTERA_HOY,
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY,
         DESCRIPCION_DEL_CORTE,
         VALOR_RECAUDO_VENCIDO,
         VALOR_VENCIDO_PERIODO)

### separamos los tipos de gestion para considerar por separado segun la subcampaña
LLAM_RECAUDO_REGIONA <- base_gestiones %>% 
  filter(ACCION=="LLAM RECAUDO REGIONA") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

LLAMADA_RECAUDO_CCTO <- base_gestiones %>% 
  filter(ACCION=="LLAMADA RECAUDO CCTO") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

LLAMADA_ENTRANTE <- base_gestiones %>% 
  filter(ACCION=="LLAMADA ENTRANTE") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

LLAM_INACT_CEL_INACT <- base_gestiones %>% 
  filter(ACCION=="LLAM INACT CEL INACT") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

LLAMADA_CASACOBRANZA <- base_gestiones %>% 
  filter(ACCION=="LLAMADA CASACOBRANZA") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

WHATSAPP <- base_gestiones %>% 
  filter(ACCION=="WHATSAPP") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

WHATSAPP_INACTIVOS <- base_gestiones %>% 
  filter(ACCION=="WHATSAPP INACTIVOS") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

GESTION_OFICINA <- base_gestiones %>% 
  filter(ACCION=="GESTION OFICINA") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

LLAMADA_RETENC_TELEF <- base_gestiones %>% 
  filter(ACCION=="LLAMADA RETENC TELEF") %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

OTRAS_GESTIONES <- base_gestiones %>% 
  filter(ACCION %in% c('ANILLO DE GEST CCTO','ENVIO COMUNICACION','INACTIVO_RETENCION')) %>% 
  select(IDENTIFICACION_DEUDOR,ACCION)

ROTACIONES <- asignacion_dia %>% 
  left_join(LLAM_RECAUDO_REGIONA %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAM_RECAUDO_REGIONA"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(LLAMADA_RECAUDO_CCTO %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAMADA_RECAUDO_CCTO"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(LLAMADA_ENTRANTE %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAMADA_ENTRANTE"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(LLAM_INACT_CEL_INACT %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAM_INACT_CEL_INACT"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(LLAMADA_CASACOBRANZA %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAMADA_CASACOBRANZA"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(WHATSAPP %>% count(IDENTIFICACION_DEUDOR,name="Q_WHATSAPP"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(WHATSAPP_INACTIVOS %>% count(IDENTIFICACION_DEUDOR,name="Q_WHATSAPP_INACTIVOS"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(GESTION_OFICINA %>% count(IDENTIFICACION_DEUDOR,name="Q_GESTION_OFICINA"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  left_join(OTRAS_GESTIONES %>% count(IDENTIFICACION_DEUDOR,name="Q_OTRAS_GESTIONES"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>% 
  left_join(LLAMADA_RETENC_TELEF %>% count(IDENTIFICACION_DEUDOR,name="Q_LLAMADA_RETENC_TELEF"),by = c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
  replace(is.na(.), 0)

ROTACIONES <- ROTACIONES %>%
  mutate(TOTAL_GESTIONES=Q_LLAM_RECAUDO_REGIONA+Q_LLAMADA_RECAUDO_CCTO+Q_LLAMADA_ENTRANTE+Q_LLAM_INACT_CEL_INACT+Q_LLAMADA_CASACOBRANZA+Q_WHATSAPP+Q_GESTION_OFICINA+Q_OTRAS_GESTIONES+Q_LLAMADA_RETENC_TELEF,
         TOTAL_LLAMADAS=Q_LLAM_RECAUDO_REGIONA+Q_LLAMADA_RECAUDO_CCTO+Q_LLAM_INACT_CEL_INACT+Q_LLAMADA_CASACOBRANZA+Q_LLAMADA_RETENC_TELEF,
         Q_ACC_POLITICA=ifelse(ACTOR_DE_GESTION_HOY=="CSS",Q_LLAMADA_RECAUDO_CCTO+Q_LLAMADA_ENTRANTE+Q_WHATSAPP+Q_WHATSAPP_INACTIVOS+Q_LLAM_RECAUDO_REGIONA,Q_LLAM_RECAUDO_REGIONA+Q_LLAMADA_ENTRANTE),
         LLAM_X_CANAL=ifelse(ACTOR_DE_GESTION_HOY=="CSS",Q_LLAMADA_RECAUDO_CCTO,Q_LLAM_RECAUDO_REGIONA),
         Q_LLAM_DIF_CANAL=TOTAL_LLAMADAS-LLAM_X_CANAL,
         Q_ACC_DIF_LLAMADA=TOTAL_GESTIONES-TOTAL_LLAMADAS,
         POLITICA_DIF_LLAMADA=Q_ACC_POLITICA-LLAM_X_CANAL)

ROTACIONES <- ROTACIONES %>%
  select(1:10,LLAM_X_CANAL,POLITICA_DIF_LLAMADA,Q_ACC_POLITICA,TOTAL_LLAMADAS,Q_ACC_DIF_LLAMADA,TOTAL_GESTIONES,Q_LLAM_DIF_CANAL)

PRIORIDAD <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Prioridad_Respuestas.xlsx")

gestiones_prioridad <- base_gestiones %>% 
  left_join(PRIORIDAD, by=c("RESPUESTA"="RESPUESTAS"))

gestiones_prioridad <- gestiones_prioridad %>%
  group_by(IDENTIFICACION_DEUDOR) %>%
  slice(which.min(PRIORIDAD))


ROTACIONES <- ROTACIONES %>% 
  left_join(gestiones_prioridad %>% select(IDENTIFICACION_DEUDOR,RESPUESTA,VALI_CONTACTO,VALI_EFECTIVIDAD),by=c("CEDULA_ASOCIADO"="IDENTIFICACION_DEUDOR")) %>%
 replace(is.na(.), "SIN GESTION")

ROTACIONES <- ROTACIONES %>%
  mutate(GESTION=ifelse(Q_ACC_POLITICA>0 | EDAD_CARTERA_HOY==0,"SI","NO"),
         POLITICA_DE_GESTION=ifelse(EDAD_CARTERA_HOY==0,"ALDIA",
                                    ifelse(EDAD_CARTERA_INICIAL==1 & Q_ACC_POLITICA>=2,"CUMPLE",
                                           ifelse(EDAD_CARTERA_INICIAL==2 & Q_ACC_POLITICA>=3, "CUMPLE",
                                                  ifelse(EDAD_CARTERA_INICIAL==3 & Q_ACC_POLITICA>=3, "CUMPLE",
                                                         ifelse(EDAD_CARTERA_INICIAL==4 & Q_ACC_POLITICA>=4, "CUMPLE",
                                                                ifelse(EDAD_CARTERA_INICIAL==0 & Q_ACC_POLITICA>=2,"CUMPLE","NO CUMPLE")))))))


ROTACIONES <- ROTACIONES %>%
  mutate(VALI_CONTACTO=ifelse(POLITICA_DE_GESTION=="ALDIA","SI",VALI_CONTACTO),
         VALI_EFECTIVIDAD=ifelse(POLITICA_DE_GESTION=="ALDIA","SI",VALI_EFECTIVIDAD))

ROTACIONES <- ROTACIONES %>%
  left_join(mp,by=c("CEDULA_ASOCIADO"="NIT")) %>% 
  mutate(MP=ifelse(is.na(MP),"0","MP")) %>% 
  left_join(cem,by=c("CEDULA_ASOCIADO"="CC_ASOCIADO")) %>% 
  mutate(CEM=ifelse(is.na(CEM),"0","CEM"))

### Se debe definir el periodo que se desea generar ## OJO CAMBIAR AL PASAR DE MES ##

mes <- my(032025)
v_start <- floor_date(mes,unit = "month")
fecha_correcta <- format(v_start, format = "%d-%m-%Y")

ROTACIONES %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Resultado/Rotaciones_{fecha_correcta}.xlsx",col_names=TRUE))


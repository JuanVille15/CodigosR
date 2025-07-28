### Script para consolidado de los nuevos Inactivos ######################################################################################################################

### Librerias
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

### Cargue de Insumos para la generacion de la base ########################################################################################################################

### Movimiento de inactivos mensual
v_today <- toTitleCase(sub("\\.$","", format(Sys.Date()-30,"%b")))
mes <- month(Sys.Date()-30)
año <- year(Sys.Date()-30)
movi_inactivos <- read_excel(glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Insumos/{mes}. Movimiento_Inactivos {v_today}_{año}.xlsx"),
                             sheet = "NVS INACTIVOS") %>% 
  clean_names(case="all_caps")

### Vista demografica Nacional - SE DEBE CARGAR EL ARCHIVO SIN FECHA
vista_demo <- read.csv2("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Insumos/bi_vistademografica_nacional.csv",header = TRUE) %>% 
  clean_names(case = "all_caps") %>% 
  transform(DOCUMENTO=as.numeric(DOCUMENTO)) %>% 
  distinct(DOCUMENTO,.keep_all = TRUE)

### consolidado de reactivaciones (Se agrupa por la ultima reactivacion)

fecha_actual <- Sys.Date()- months(18)
periodo_considerar <-floor_date(fecha_actual, unit = "month")

consol_reac <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Insumos/Info Asociados Reactivados consolidada.xlsx",
                          sheet = "Base Reactivados 2020-2024") %>% 
  clean_names(case = "all_caps") %>% 
  filter(ESTRATEGIA_DE_REACTIVACION!=is.na(ESTRATEGIA_DE_REACTIVACION),
         MES_REACTIVACION>= {periodo_considerar}) %>%
  group_by(CEDULA) %>%
  slice(which.max(MES_REACTIVACION))

### Base Motivos de No pago ultimo año (se agrupa por ultimo motivo registrado)

motivos <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Motivos.xlsx")
v_start <- floor_date(Sys.Date()-365, unit = "month")

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KyCo_090625*")

motivo_no_pago <- tbl(Connecion_SQL, in_schema("GRC", "GCC_HISTORIA_GESTIONES")) %>%
  filter(
    GES_FECHA_GESTION >= TO_DATE(v_start, 'YYYY-MM-DD'),
    !is.na(MOTV_NOMBRE_MOTIVO),
    MOTV_NOMBRE_MOTIVO !="-"
  ) %>%
  select(
    ASO_IDENTIFICACION,
    GES_FECHA_GESTION,
    MOTV_NOMBRE_MOTIVO
  ) %>%
  collect() %>%
  clean_names(case = "all_caps")

motivo_no_pago <- motivo_no_pago %>%
  filter(MOTV_NOMBRE_MOTIVO %in% c(motivos$MOTV_NOMBRE_MOTIVO)) %>%
  group_by(ASO_IDENTIFICACION) %>%
  slice(which.max(GES_FECHA_GESTION))

dbDisconnect(Connecion_SQL)

### Consolidado de segmentaciones mensuales

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Insumos/Segmentacion Mesual"
archivos <- list.files(path = ruta_carpeta, full.names = TRUE)
segm_consol <- bind_rows(lapply(archivos, read_excel))%>%
  transform(PERIODO=as.numeric(PERIODO)) %>% 
  group_by(DOCUMENTO) %>%
  slice(which.max(PERIODO))%>% 
  clean_names(case="all_caps")

### Cargue reporte 10 de cierre SE DEBE CAMBAIR LA RUTA CADA MES

ruta_carpeta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
reporte_10_cierre <- read_excel(ultimo_archivo) %>% 
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)

### Cargue segmentacion completa inactivos

ruta_carpeta <- "//coomeva.nal/dfscoomeva/Conocimiento_Asociado_GCCA/salida/segmentacion_inactivos/bajo_demanda"
archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
segm_total <- read_excel(ultimo_archivo) %>% 
  clean_names(case="all_caps")

### Unificacion de la informacion requerida ################################################################################################################################

mes <- Sys.Date()-30
v_start <- floor_date(mes,unit = "month")
fecha_correcta <- format(v_start, format = "%d-%m-%Y")

nvs <- movi_inactivos %>% 
  select(OFICINA,ZONA,REGIONAL,CEDULA,FECHA_CORTE,ESTADO_ASOCIATIVIDAD)
nvs <- nvs %>% 
  left_join(motivo_no_pago %>% select(ASO_IDENTIFICACION,MOTV_NOMBRE_MOTIVO), by=c("CEDULA"="ASO_IDENTIFICACION")) %>% 
  mutate(MOTV_NOMBRE_MOTIVO=ifelse(is.na(MOTV_NOMBRE_MOTIVO),"NO",MOTV_NOMBRE_MOTIVO)) %>% 
  left_join(vista_demo %>% select(DOCUMENTO,
                                  TIPO_DOCUMENTO,
                                  INGRESOS,
                                  PTAJE_ACIERTA,
                                  SUMA_PRODUCTOS),by=c("CEDULA"="DOCUMENTO")) %>% 
  mutate(TIPO_DOCUMENTO=ifelse(is.na(TIPO_DOCUMENTO),"CC",TIPO_DOCUMENTO)) %>% 
  mutate(INGRESOS=ifelse(is.na(INGRESOS),0,INGRESOS)) %>% 
  mutate(PTAJE_ACIERTA=ifelse(is.na(PTAJE_ACIERTA),0,PTAJE_ACIERTA)) %>% 
  mutate(SUMA_PRODUCTOS=ifelse(is.na(SUMA_PRODUCTOS),0,SUMA_PRODUCTOS)) %>% 
  left_join(reporte_10_cierre %>% select(CEDULA_ASOCIADO,
                                         FECHA_INGRESO_ASOCIADO,
                                         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA,
                                         NO_CUOTAS_CANCELADAS,
                                         EDAD_ASOCIADO),by=c("CEDULA"="CEDULA_ASOCIADO")) %>% 
  transform(FECHA_INGRESO_ASOCIADO=as.character.Date(FECHA_INGRESO_ASOCIADO)) %>% 
  mutate(FECHA_INGRESO_ASOCIADO=ifelse(is.na(FECHA_INGRESO_ASOCIADO),"NA",FECHA_INGRESO_ASOCIADO)) %>% 
  mutate(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA=ifelse(is.na(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA),0,ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA)) %>% 
  mutate(NO_CUOTAS_CANCELADAS=ifelse(is.na(NO_CUOTAS_CANCELADAS),0,NO_CUOTAS_CANCELADAS)) %>% 
  mutate(EDAD_ASOCIADO=ifelse(is.na(EDAD_ASOCIADO),0,EDAD_ASOCIADO)) %>% 
  left_join(consol_reac %>% select(CEDULA,
                                   MES_REACTIVACION,
                                   ESTRATEGIA_DE_REACTIVACION),by=c("CEDULA"="CEDULA")) %>% 
  transform(MES_REACTIVACION=as.character.Date(MES_REACTIVACION)) %>% 
  mutate(MES_REACTIVACION=ifelse(is.na(MES_REACTIVACION),"NO",MES_REACTIVACION)) %>% 
  mutate(ESTRATEGIA_DE_REACTIVACION=ifelse(is.na(ESTRATEGIA_DE_REACTIVACION),"NO",ESTRATEGIA_DE_REACTIVACION)) %>% 
  mutate(PERIODO=fecha_correcta) %>% 
  mutate(REINCIDENTE=ifelse(ESTRATEGIA_DE_REACTIVACION!="NO","REINCIDENTE","NO REINCIDENTE"))

reincidente <- nvs %>% 
  filter(REINCIDENTE=="REINCIDENTE") %>% 
  left_join(segm_consol %>% select(DOCUMENTO,
                                   SEGMENTO_AJUSTADO),by=c("CEDULA"="DOCUMENTO")) %>% 
  mutate(SEGMENTO_AJUSTADO=ifelse(is.na(SEGMENTO_AJUSTADO),"SIN SEGMENTO",SEGMENTO_AJUSTADO)) %>% 
  mutate(SEGMENTO_COLOR=ifelse(startsWith(SEGMENTO_AJUSTADO, "Verde"),"Verde",
                               ifelse(startsWith(SEGMENTO_AJUSTADO, "Amarillo"),"Amarillo",
                                      ifelse(startsWith(SEGMENTO_AJUSTADO, "Rojo"),"Rojo","Sin segmento"))))
segm_total <- segm_total %>% 
  transform(DOCUMENTO=as.numeric(DOCUMENTO))

nvs <- nvs %>%
  left_join(reincidente %>% select(CEDULA,
                                   SEGMENTO_AJUSTADO,
                                   SEGMENTO_COLOR), by=c("CEDULA"="CEDULA")) %>% 
  mutate(SEGMENTO_AJUSTADO=ifelse(is.na(SEGMENTO_AJUSTADO),"NO",SEGMENTO_AJUSTADO),
         SEGMENTO_COLOR=ifelse(is.na(SEGMENTO_COLOR),"NO",SEGMENTO_COLOR)) %>% 
  rename(SEGMENTO=SEGMENTO_AJUSTADO) %>% 
  left_join(segm_total %>% select(DOCUMENTO,
                                  SEGMENTO_AJUSTADO),by=c("CEDULA"="DOCUMENTO")) %>% 
  mutate(SEGMENTO_AJUSTADO=ifelse(startsWith(SEGMENTO_AJUSTADO, "Verde")|startsWith(SEGMENTO_AJUSTADO, "Amarillo"),"Alta probabilidad","Baja probabilidad"))

### Rangos de la informacion ###############################################################################################################################################

nvs <- nvs %>%
  mutate(AGRUPACION_ANTIGUEDAD=ifelse(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA<=2,"<= 2 Anos",
                                      ifelse(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA>2 & ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA<=5,"Entre 3 y 5 Anos",
                                             ifelse(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA>5 & ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA<=10,"Entre 5 y 10 Anos",
                                                    ifelse(ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA>10 & ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA<= 20,"Entre 10 y 20 Anos","> 20 Anos"))))) %>% 
  mutate(AGRUPACION_EDAD=ifelse(EDAD_ASOCIADO<20,"Menor a 20 Anos",
                                ifelse(EDAD_ASOCIADO>=20 & EDAD_ASOCIADO<=30,"De 20 a 30 Anos",
                                       ifelse(EDAD_ASOCIADO>30 & EDAD_ASOCIADO<=35,"De 31 a 35 Anos",
                                              ifelse(EDAD_ASOCIADO>35 & EDAD_ASOCIADO<= 54,"De 36 a 54 Anos",
                                                     ifelse(EDAD_ASOCIADO>54 & EDAD_ASOCIADO<= 60,"De 55 a 60 Anos","> 60 Anos")))))) %>% 
  mutate(AGRUPACION_INGRESOS=ifelse(INGRESOS<1800000,"< $1.8 MM",
                                    ifelse(INGRESOS>=1800000 & INGRESOS<=2000000,"De $1.8 a $2 MM",
                                           ifelse(INGRESOS>2000000 & INGRESOS<=4000000,"De $2 a $4 MM",
                                                  ifelse(INGRESOS>4000000 & INGRESOS<= 7000000,"De $4 a $7 MM",
                                                         ifelse(INGRESOS>7000000 & INGRESOS<= 14000000,"De $7 a $14 MM",
                                                                ifelse(INGRESOS>14000000 & INGRESOS<= 20000000,"De $14 a $20 MM","> $20 MM"))))))) %>% 
  mutate(AGRUPACION_ACIERTA=ifelse(PTAJE_ACIERTA==0,"0",
                                   ifelse(PTAJE_ACIERTA>0 & PTAJE_ACIERTA<=600,"Sin perfil 1 a 600",
                                          ifelse(PTAJE_ACIERTA>600 & PTAJE_ACIERTA<=620,"Sin perfil 601 a 620",
                                                 ifelse(PTAJE_ACIERTA>620 & PTAJE_ACIERTA<= 699,"Sin perfil 621 a 699","Con perfil >= 700"))))) %>% 
  mutate(AGRUPACION_CUOTAS_PAGADAS=ifelse(NO_CUOTAS_CANCELADAS==0,"0",
                                          ifelse(NO_CUOTAS_CANCELADAS>0 & NO_CUOTAS_CANCELADAS<=6,"De 1 a 6 cuotas",
                                                 ifelse(NO_CUOTAS_CANCELADAS>6 & NO_CUOTAS_CANCELADAS<=24,"De 7 a 24 cuotas",
                                                        ifelse(NO_CUOTAS_CANCELADAS>24 & NO_CUOTAS_CANCELADAS<= 60,"De 25 a 60 cuotas",
                                                               ifelse(NO_CUOTAS_CANCELADAS>60 & NO_CUOTAS_CANCELADAS<= 120,"De 61 a 120 cuotas",
                                                                      ifelse(NO_CUOTAS_CANCELADAS>120 & NO_CUOTAS_CANCELADAS<= 240,"De 121 a 240 cuotas","> 240 cuotas"))))))) %>% 
  mutate(AGRUPACION_TENENCIA=ifelse(SUMA_PRODUCTOS<=1,"De 0 a 1 Producto",
                                    ifelse(SUMA_PRODUCTOS>1 & SUMA_PRODUCTOS<=3,"De 2 a 3 Productos",
                                           ifelse(SUMA_PRODUCTOS>3 & SUMA_PRODUCTOS<=5,"De 4 a 5 Productos","> 5 Productos"))))  %>% 
  mutate(REACTIVACION_INTRAMES=ifelse(ESTADO_ASOCIATIVIDAD=="Inactivo","NO","SI"),
         TIPO_PERSONA=ifelse(TIPO_DOCUMENTO=="NIT","Pj","Pn")) %>% 
  select(-TIPO_DOCUMENTO)


nvs <- nvs %>%
  mutate(SCORE=NA)

nvs <- nvs %>%
  select(PERIODO,
         OFICINA,
         ZONA,
         REGIONAL,
         CEDULA,
         MOTV_NOMBRE_MOTIVO,
         TIPO_PERSONA,
         FECHA_CORTE,
         REACTIVACION_INTRAMES,
         FECHA_INGRESO_ASOCIADO,
         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA,
         ESTADO_ASOCIATIVIDAD,
         NO_CUOTAS_CANCELADAS,
         EDAD_ASOCIADO,
         INGRESOS,
         SCORE,
         PTAJE_ACIERTA,
         SUMA_PRODUCTOS,
         REINCIDENTE,
         MES_REACTIVACION,
         ESTRATEGIA_DE_REACTIVACION,
         SEGMENTO,
         SEGMENTO_COLOR,
         AGRUPACION_ANTIGUEDAD,
         AGRUPACION_EDAD,
         AGRUPACION_INGRESOS,
         AGRUPACION_ACIERTA,
         AGRUPACION_CUOTAS_PAGADAS,
         AGRUPACION_TENENCIA,
         SEGMENTO_AJUSTADO)

## Se trae el Perfil del asociado ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Perfil Asociado"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Perfil <- read_excel(ultimo_archivo)

## Cruzamos el Perfil ##

nvs <- nvs %>% left_join(Perfil %>% select(strIdentificacion,strAgrupacionPerfil), by = c("CEDULA" = "strIdentificacion"))

#Escribimos el archivo final ##

write_xlsx(nvs,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Nuevos Inactivos/Resultados/Nuevos inactivos.xlsx")

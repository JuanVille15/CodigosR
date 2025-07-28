### Informes de ROdamiento #################################################################################

### Librerias ##############################################################################################
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
library(blastula)
library(janitor)

##conflicts_prefer(dplyr::filter)
### Vector de Meses en español para definir los reportes a cargar###########################################

meses_espanol <- c("Enero",
                   "Febrero",
                   "Marzo",
                   "Abril",
                   "Mayo",
                   "Junio",
                   "Julio",
                   "Agosto",
                   "Septiembre",
                   "Octubre",
                   "Noviembre",
                   "Diciembre")

### Importante definir la fecha del reporte que se va a generar#############################################
fecha <- my(062025)

mes <- month(fecha)
año <- year(fecha)
nombre_mes <- meses_espanol[mes]

mes <- month((fecha)-months(1))
año1 <- year((fecha)-months(1))
nombre_mes1 <- meses_espanol[mes]

mes <- month((fecha)-months(2))
año2 <- year((fecha)-months(2))
nombre_mes2 <- meses_espanol[mes]

mes <- month((fecha)-months(3))
año3 <- year((fecha)-months(3))
nombre_mes3 <- meses_espanol[mes]

### Subcamapañas que reciben los nuevos rodados#############################################################

nvs_rodados <- c('BANCOOMEVA_U',
                 'CSSCUOTA0HASTA6',
                 'CSSMORA1CORTE10',
                 'CSSMORA1CORTE15',
                 'CSSMORA1CORTE20',
                 'CSSMORA1CORTE25',
                 'CSSMORA1CORTE5')

### Cargue de Insumos para la generacion de la Base

### Reporte 10 de cierre y los ultimos reporte anteriores al periodo de cierre ################################

r10_periodo <- read_excel(glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Insumos/R10_Cierre_{nombre_mes}_{año}.xlsx")) %>%
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE) %>% 
  select(REGIONAL,
         ZONA,
         NOMBRE_OFICINA,
         CEDULA_ASOCIADO,
         EDAD_ASOCIADO,
         NO_CUOTAS_CANCELADAS,
         VALOR_VENCIDO_HOY,
         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA,
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY,
         ACTOR_DE_GESTION_HOY,
         SUBCAMPANA,
         TIPO_CLIENTE,
         FECHA_INGRESO_ASOCIADO,
         EDAD_CARTERA_INICIAL,
         EDAD_CARTERA_HOY,
         DESCRIPCION_DEL_CORTE) %>% 
  mutate(CATEGORIA=ifelse(EDAD_CARTERA_INICIAL==0 & EDAD_CARTERA_HOY==0 & SUBCAMPANA %in% nvs_rodados,"NORMALIZA",
                          ifelse(EDAD_CARTERA_INICIAL> 0 & EDAD_CARTERA_HOY==0,"NORMALIZA",
                                 ifelse(EDAD_CARTERA_INICIAL==0 & EDAD_CARTERA_HOY==0,"ALDIA",
                                        ifelse(EDAD_CARTERA_HOY < EDAD_CARTERA_INICIAL & EDAD_CARTERA_HOY <= 4, "MEJORA",
                                               ifelse(EDAD_CARTERA_INICIAL > 0 & EDAD_CARTERA_INICIAL== EDAD_CARTERA_HOY, "MANTIENE",
                                                      ifelse(EDAD_CARTERA_INICIAL<EDAD_CARTERA_HOY,"DETERIORA","MANTIENE")))))),
         EDAD_ASOCIADO=ifelse(is.na(EDAD_ASOCIADO),0,EDAD_ASOCIADO))

r10_periodo1 <- read_excel(glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Insumos/R10_Cierre_{nombre_mes1}_{año1}.xlsx")) %>%
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)%>%
  select(CEDULA_ASOCIADO,
         EDAD_CARTERA_HOY) %>% 
  rename(EDAD_CARTERA_HOY_1=EDAD_CARTERA_HOY)

r10_periodo2 <- read_excel(glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Insumos/R10_Cierre_{nombre_mes2}_{año2}.xlsx")) %>%
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)%>%
  select(CEDULA_ASOCIADO,
         EDAD_CARTERA_HOY) %>% 
  rename(EDAD_CARTERA_HOY_2=EDAD_CARTERA_HOY)


r10_periodo3 <- read_excel(glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Insumos/R10_Cierre_{nombre_mes3}_{año3}.xlsx"))%>%
  clean_names(case="all_caps") %>% 
  distinct(CEDULA_ASOCIADO,.keep_all = TRUE)%>% 
  select(CEDULA_ASOCIADO,
         EDAD_CARTERA_INICIAL,
         EDAD_CARTERA_HOY) %>% 
  rename(EDAD_CARTERA_HOY_3=EDAD_CARTERA_HOY,
         EDAD_CARTERA_HOY_4=EDAD_CARTERA_INICIAL)

### Se realizan las transformaciones a la informacion######################################################

r10_periodo_analisis <- r10_periodo %>% 
  left_join(r10_periodo1, by=c("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>% 
  left_join(r10_periodo2, by=c("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>% 
  left_join(r10_periodo3, by=c("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>% 
  mutate(EDAD_CARTERA_HOY=ifelse(is.na(EDAD_CARTERA_HOY),0,EDAD_CARTERA_HOY),
         EDAD_CARTERA_HOY_1=ifelse(is.na(EDAD_CARTERA_HOY_1),0,EDAD_CARTERA_HOY_1),
         EDAD_CARTERA_HOY_2=ifelse(is.na(EDAD_CARTERA_HOY_2),0,EDAD_CARTERA_HOY_2),
         EDAD_CARTERA_HOY_3=ifelse(is.na(EDAD_CARTERA_HOY_3),0,EDAD_CARTERA_HOY_3),
         EDAD_CARTERA_HOY_4=ifelse(is.na(EDAD_CARTERA_HOY_4),0,EDAD_CARTERA_HOY_4))

r10_periodo_analisis <- r10_periodo_analisis %>%
  mutate(CANTIDAD_PERIODOS_NORMALIZADOS = rowSums(select(., starts_with("EDAD_CARTERA_HOY")) == 0))

r10_periodo_analisis <- r10_periodo_analisis %>% 
  mutate(REINCIDENTE_MORA=ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==5,"ALDIA",
                                 ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==4 & EDAD_CARTERA_HOY != 0,"NUEVO RODADO",
                                        ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==4 & EDAD_CARTERA_HOY==0,"REINCIDE TEMPORAL",
                                               ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==3, "REINCIDE_2",
                                                      ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==2, "REINCIDE_3",
                                                             ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==1, "REINCIDE_4",
                                                                    ifelse(CANTIDAD_PERIODOS_NORMALIZADOS==0, "DETERIORO_MANTENIDO","REVISAR"))))))))

### Se carga el archivo de Vista Demografica mas actualizado  ACTUALIZAR NOMBRE!!! #################################################

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Conocimiento_Asociado_GCCA/salida/Tabla_Demografica_Tenencia_Para_Consumo/Nacional/bi_vistademografica_nacional_2025-06-11.csv"
vista_demo <- read.csv2(ruta_carpeta) %>% 
  clean_names(case = "all_caps") %>% 
  transform(DOCUMENTO=as.numeric(DOCUMENTO)) %>% 
  distinct(DOCUMENTO,.keep_all = TRUE)

r10_periodo_analisis <- r10_periodo_analisis %>%
  left_join(vista_demo %>% select(DOCUMENTO,
                                  TIPO_DOCUMENTO,
                                  INGRESOS,
                                  PTAJE_ACIERTA,
                                  SUMA_PRODUCTOS,
                                  NOMBRE_NIVEL_ACADEMICO),by=c("CEDULA_ASOCIADO"="DOCUMENTO"))%>% 
  mutate(TIPO_DOCUMENTO=ifelse(is.na(TIPO_DOCUMENTO),"CC",TIPO_DOCUMENTO)) %>% 
  mutate(INGRESOS=ifelse(is.na(INGRESOS),0,INGRESOS)) %>% 
  mutate(PTAJE_ACIERTA=ifelse(is.na(PTAJE_ACIERTA),0,PTAJE_ACIERTA)) %>% 
  mutate(SUMA_PRODUCTOS=ifelse(is.na(SUMA_PRODUCTOS),0,SUMA_PRODUCTOS))

### consolidado de reactivaciones (Se agrupa por la ultima reactivacion)#####################################

### rango de tiempo a considerar para marcar como reincidente
fecha_actual <- Sys.Date()- months(18)
periodo_considerar <-floor_date(fecha_actual, unit = "month")

consol_reac <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Proceso Inactivos/Reactivados por mes estrategia/Info Asociados Reactivados consolidada.xlsx",
                          sheet = "Base Reactivados 2020-2024") %>% 
  clean_names(case = "all_caps") %>% 
  filter(ESTRATEGIA_DE_REACTIVACION!=is.na(ESTRATEGIA_DE_REACTIVACION),
         MES_REACTIVACION>= {periodo_considerar}) %>%
  group_by(CEDULA) %>%
  slice(which.max(MES_REACTIVACION))

r10_periodo_analisis <- r10_periodo_analisis %>%
  left_join(consol_reac %>% select(CEDULA,
                                   MES_REACTIVACION,
                                   ESTRATEGIA_DE_REACTIVACION),by=c("CEDULA_ASOCIADO"="CEDULA"))%>% 
  mutate(ESTRATEGIA_DE_REACTIVACION=ifelse(is.na(ESTRATEGIA_DE_REACTIVACION),"NO",ESTRATEGIA_DE_REACTIVACION)) %>%
  mutate(REINCIDENTE_INAC=ifelse(ESTRATEGIA_DE_REACTIVACION!="NO","REINCIDENTE","NO REINCIDENTE"))

### Cargue segmentacion de habito de pago ###################################################################

ruta_carpeta <- "//coomeva.nal/dfscoomeva/Conocimiento_Asociado_GCCA/salida/segmentacion_habito_pago/bd_patrones_pago_202504_EST_USO-INTERNO.xlsx"
seg_habito <- read_excel(ruta_carpeta)%>% 
  clean_names(case = "all_caps") %>% 
  transform(NITCDL_ASOCIADO=as.numeric(NITCDL_ASOCIADO)) %>% 
  distinct(NITCDL_ASOCIADO,.keep_all = TRUE)

r10_periodo_analisis <- r10_periodo_analisis %>%
  left_join(seg_habito %>% select(NITCDL_ASOCIADO,
                                  SEGMENTO_HABITO_3_TOTAL_AJUSTADO),by=c("CEDULA_ASOCIADO"="NITCDL_ASOCIADO")) %>% 
  mutate(SEGMENTO_HABITO_3_TOTAL_AJUSTADO=ifelse(is.na(SEGMENTO_HABITO_3_TOTAL_AJUSTADO),"MuyDeficiente_Impago",SEGMENTO_HABITO_3_TOTAL_AJUSTADO))

### DEFINIR LOS RANGOS PARA LAS TABLAS#######################################################################

r10_periodo_analisis <- r10_periodo_analisis %>%
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
                                           ifelse(SUMA_PRODUCTOS>3 & SUMA_PRODUCTOS<=5,"De 4 a 5 Productos","> 5 Productos")))) %>% 
  mutate(TIPO_PERSONA=ifelse(TIPO_DOCUMENTO=="NIT","Pj","Pn")) %>% 
  select(-TIPO_DOCUMENTO) %>% 
  mutate(PERFIL=ifelse(INGRESOS>=4000000 & PTAJE_ACIERTA>=700 & EDAD_ASOCIADO>=20 & NOMBRE_NIVEL_ACADEMICO=="Profesional","PERFIL 1",
                       ifelse(INGRESOS>=2000000 & INGRESOS<4000000 & PTAJE_ACIERTA>=700 & EDAD_ASOCIADO>=35 & EDAD_ASOCIADO<=54 & NOMBRE_NIVEL_ACADEMICO=="Profesional","PERFIL 2",
                              ifelse(INGRESOS>=1800000 & (PTAJE_ACIERTA>=700 | PTAJE_ACIERTA==0) & EDAD_ASOCIADO>=20 & EDAD_ASOCIADO<=30 & NOMBRE_NIVEL_ACADEMICO=="Profesional", "PERFIL 3", "OTRO PERFIL")))) %>% 
  mutate(MOTIVO_RODAMIENTO=ifelse(CATEGORIA=="DETERIORA" & NO_CUOTAS_CANCELADAS==0,"CUOTA CERO",
                                  ifelse(CATEGORIA=="DETERIORA" & REINCIDENTE_INAC == "REINCIDENTE","REINCIDENTE_INAC",
                                         ifelse(CATEGORIA=="DETERIORA","RODAMIENTO NATURAL","CONTENIDO"))))


### Base Motivos de No pago ultimo año (se agrupa por ultimo motivo registrado)################################

motivos <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Motivos.xlsx")
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

r10_periodo_analisis <- r10_periodo_analisis %>% 
  left_join(motivo_no_pago %>% select(ASO_IDENTIFICACION,
                                      MOTV_NOMBRE_MOTIVO), by=c("CEDULA_ASOCIADO"="ASO_IDENTIFICACION")) %>% 
  mutate(MOTV_NOMBRE_MOTIVO=ifelse(is.na(MOTV_NOMBRE_MOTIVO),"NO",MOTV_NOMBRE_MOTIVO))

### Se organiza la informacion para generar la base final #######################################################

r10_periodo_analisis <- r10_periodo_analisis %>%
  mutate(PERIODO={fecha}) %>%
  filter(DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY %in% c("Activo Normal",
                                                       "Activo Cobranza Interna",
                                                       "Inactivo",
                                                       "Receso"),
         TIPO_CLIENTE %in% c("Mixto (Asociado/Cliente)",
                             "Solo Asociado")) %>% 
  mutate(VALOR_VENCIDO_HOY=ifelse(EDAD_CARTERA_HOY==0,0,VALOR_VENCIDO_HOY)) %>% 
  select(PERIODO,
         NOMBRE_OFICINA,
         ZONA,
         REGIONAL,
         CEDULA_ASOCIADO,
         MOTV_NOMBRE_MOTIVO,
         TIPO_PERSONA,
         TIPO_CLIENTE,
         NOMBRE_NIVEL_ACADEMICO,
         ACTOR_DE_GESTION_HOY,
         SUBCAMPANA,
         VALOR_VENCIDO_HOY,
         DESCRIPCION_DEL_CORTE,
         EDAD_CARTERA_INICIAL,
         EDAD_CARTERA_HOY,
         CATEGORIA,
         FECHA_INGRESO_ASOCIADO,
         ANTIGUEDAD_DEL_ASOCIADO_EN_COOMEVA,
         DESCRIPCION_DEL_ESTADO_MULTIACTIVA_HOY,
         NO_CUOTAS_CANCELADAS,
         EDAD_ASOCIADO,
         INGRESOS,
         PTAJE_ACIERTA,
         SUMA_PRODUCTOS,
         REINCIDENTE_INAC,
         MES_REACTIVACION,
         ESTRATEGIA_DE_REACTIVACION,
         SEGMENTO_HABITO_3_TOTAL_AJUSTADO,
         AGRUPACION_ANTIGUEDAD,
         AGRUPACION_EDAD,
         AGRUPACION_INGRESOS,
         AGRUPACION_ACIERTA,
         AGRUPACION_CUOTAS_PAGADAS,
         AGRUPACION_TENENCIA,
         CANTIDAD_PERIODOS_NORMALIZADOS,
         REINCIDENTE_MORA,
         PERFIL,
         MOTIVO_RODAMIENTO)


## Se trae el Perfil del asociado ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Perfil Asociado"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Perfil <- read_excel(ultimo_archivo)

## Cruzamos el Perfil ##

r10_periodo_analisis <- r10_periodo_analisis %>% left_join(Perfil %>% select(strIdentificacion,strAgrupacionPerfil), by = c("CEDULA_ASOCIADO" = "strIdentificacion"))


r10_periodo_analisis <- r10_periodo_analisis %>% rename(Perfil_Asociado = strAgrupacionPerfil)

write_xlsx(r10_periodo_analisis,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Informes de Cierre MT/Informe de rodamiento/Resultado/Resultado_{nombre_mes}_{año}.xlsx"))

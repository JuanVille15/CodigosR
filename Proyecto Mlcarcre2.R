#### SEGUIMIENTO INDICE DE RECAUDO Y CONTENCION CREDITOS POR CANAL ###

### LIBRERIAS NECESARIAS ####

library(tidyverse)
library(readxl)
library(writexl)
library(dplyr)
library(glue)
library(dplyr)
library(DBI)
library(odbc)
library(lubridate)
library(janitor)
library(dbplyr)
library(tools)
library(stringi)
library(readr)


#   1.       -------- GENERAR ASIGNACION CONSOLIDADA -----------  #

ASIGNACION <- read_csv("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Insumos/Asiganciones Cierre/ASIGNACION.csv")

## Leemos la asignación Diaria
ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

ASIGNACION1 <- read_excel(ultimo_archivo)


ASIGNACION1 <- ASIGNACION1 %>% filter(REGIONAL != is.null(REGIONAL)) %>% 
  rename(CEDULA_ASOCIADO = `CEDULA ASOCIADO`,
         ACTOR_DE_GESTION_HOY = `ACTOR DE GESTIÓN HOY`,
         SUBCAMPANA = SUBCAMPAÑA) %>% 
  mutate(PERIODO = '202506') ## Cambiar Periodo Corriente ##

ASIGNACION1 <- ASIGNACION1 %>% select(CEDULA_ASOCIADO,ACTOR_DE_GESTION_HOY,SUBCAMPANA,PERIODO)


ASIGNACION <- ASIGNACION %>% filter(PERIODO != 202506) ## Cambiar Periodo Corriente ## 

ASIGNACION <- rbind(ASIGNACION1,ASIGNACION) %>% 
  distinct(CEDULA_ASOCIADO,PERIODO, .keep_all = TRUE)

write_csv(ASIGNACION, "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Insumos/Asiganciones Cierre/ASIGNACION.csv")


## 2.  ---------- GENERAR CONSOLIDADO DE CIERRE --------- ##


#### TABLAS DE HOMOLOGACION #####

LINEA <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Descriptiva Creditos/Insumos/Homologacion_Credito.xlsx")

ESTADO <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Descriptiva Creditos/Insumos/Homologacion_Estado.xlsx")

ASIGNA <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Descriptiva Creditos/Insumos/Homologacion_Asignacion.xlsx") ## Por modificar ##

### Importante cambiar la fecha de la carpeta para la carga de comunicaciones ######

directorio <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Insumos/Mlcarcre Inicial" ## PEGAR MLCARCRE INICIAL DEL PERIODO EN ESTA RUTA ##

# Obtener la lista de archivos en el directorio ##################################
archivos <- list.files(directorio, full.names = TRUE)

# Función para cargar un archivo y agregar el nombre del archivo como una columna
cargar_con_nombre <- function(archivo) {
  # Cargar el archivo
  df <- read_excel(archivo) %>% select(PEREVA,
                                       NOMREG,
                                       NOMZON,
                                       NOMOFI,
                                       NITCLI,
                                       NOMCLI,
                                       DESESTA,
                                       CODCPT,
                                       WNROPAG,
                                       NROCTAV,
                                       VLRVCDT,
                                       SDOOBLGTOT,
                                       DIASVEN,
                                       WDESCOR,
                                       WFECPROA)
  
  # Obtener el nombre del archivo
  nombre_archivo <- basename(archivo)
  
  # Agregar el nombre del archivo como una columna
  df$NOMBRE_ARCHIVO <- nombre_archivo
  
  # Retornar el dataframe modificado
  return(df)
}

# Utilizar lapply para aplicar la función a cada archivo
dataframes <- lapply(archivos, cargar_con_nombre)

# Combinar todos los dataframes en uno solo
MLCARCRE_INI <- do.call(rbind, dataframes) %>% 
  select(-NOMBRE_ARCHIVO) %>% 
  mutate(LLAVE= case_when(nchar(WNROPAG)== 8 ~ paste0(substr(WNROPAG,1,7),"0"),
                          TRUE ~ paste0(substr(WNROPAG,1,8),"00"))) %>%
  transform(LLAVE=as.numeric(LLAVE)) %>% 
  mutate(EDAD_DE_MORA_INI = cut(
    DIASVEN,
    breaks = c(0, seq(1, 10021 + 30, by = 30)),
    labels = paste(0:(length(seq(1, 10021, by = 30)))),
    right = FALSE)) %>%
  transform(EDAD_DE_MORA_INI = as.numeric(as.character(EDAD_DE_MORA_INI))) %>% 
  mutate(EDAD_DE_MORA_INI = case_when(NROCTAV > EDAD_DE_MORA_INI ~ NROCTAV,
                                      TRUE ~ EDAD_DE_MORA_INI)) %>% 
  select(-NROCTAV) %>%
  distinct(PEREVA, NITCLI, LLAVE, .keep_all = TRUE)

names(MLCARCRE_INI) <- c("PEREVA", "REGIONAL", "ZONA","OFICINA","NITCLI","NOMCLI","DESESTA_INI","CODCPT","WNROPAG","VENCIDO_PERIODO","SALDO_TOTAL","DIAS_DE_MORA_INI","CORTE","FECHA_ACT_INI","LLAVE","EDAD_DE_MORA_INI")

directorio <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Insumos/Mlcarcre Cierre" ## PEGAR ARCHIVO MLCARCRE DIARIO EN ESTA RUTA ##
# Obtener la lista de archivos en el directorio ##################################
archivos <- list.files(directorio, full.names = TRUE)

# Función para cargar un archivo y agregar el nombre del archivo como una columna
cargar_con_nombre <- function(archivo) {
  # Cargar el archivo
  df <- read_excel(archivo) %>% select(PEREVA,
                                       NITCLI,
                                       DESESTA,
                                       WNROPAG,
                                       NROCTAV,
                                       VLRVCDT,
                                       RECVCDO,
                                       DIASVEN,
                                       WFECPROA)
  
  # Obtener el nombre del archivo
  nombre_archivo <- basename(archivo)
  
  # Agregar el nombre del archivo como una columna
  df$NOMBRE_ARCHIVO <- nombre_archivo
  
  # Retornar el dataframe modificado
  return(df)
}

# Utilizar lapply para aplicar la función a cada archivo
dataframes <- lapply(archivos, cargar_con_nombre)

# Combinar todos los dataframes en uno solo
MLCARCRE_CIERRE <- do.call(rbind, dataframes) %>% 
  select(-NOMBRE_ARCHIVO) %>% 
  mutate(LLAVE= case_when(nchar(WNROPAG)== 8 ~ paste0(substr(WNROPAG,1,7),"0"),
                          TRUE ~ paste0(substr(WNROPAG,1,8),"00"))) %>%
  transform(LLAVE=as.numeric(LLAVE)) %>% 
  mutate(EDAD_DE_MORA_FIN = cut(
    DIASVEN,
    breaks = c(0, seq(1, 10021 + 30, by = 30)),
    labels = paste(0:(length(seq(1, 10021, by = 30)))),
    right = FALSE)) %>%
  transform(EDAD_DE_MORA_FIN = as.numeric(as.character(EDAD_DE_MORA_FIN))) %>% 
  mutate(EDAD_DE_MORA_FIN = case_when(NROCTAV > EDAD_DE_MORA_FIN ~ NROCTAV,
                                      TRUE ~ EDAD_DE_MORA_FIN)) %>% 
  select(-NROCTAV,-WNROPAG) %>%
  distinct(PEREVA, NITCLI, LLAVE, .keep_all = TRUE)

names(MLCARCRE_CIERRE) <- c("PEREVA","NITCLI","DESESTA_FIN","VALOR_VENCIDO_HOY","RECAUDO_VENCIDO","DIAS_DE_MORA_FIN","FECHA_ACT_FIN","LLAVE","EDAD_DE_MORA_FIN")


MLCARCRE_COMPLETO <- MLCARCRE_INI %>% 
  left_join(MLCARCRE_CIERRE, by = c("PEREVA"="PEREVA","NITCLI"="NITCLI","LLAVE"="LLAVE")) %>% 
  mutate(VENCIDO_PERIODO = case_when(EDAD_DE_MORA_INI == 0 ~ VALOR_VENCIDO_HOY + RECAUDO_VENCIDO,
                                     TRUE ~ VENCIDO_PERIODO),
         DESESTA_FIN = case_when(is.na(DESESTA_FIN) ~ "PREPAGO",
                                 TRUE ~ DESESTA_FIN),
         VALOR_VENCIDO_HOY = case_when(is.na(VALOR_VENCIDO_HOY) ~ 0,
                                       TRUE ~ VALOR_VENCIDO_HOY),
         RECAUDO_VENCIDO = case_when(is.na(RECAUDO_VENCIDO) ~ VENCIDO_PERIODO,
                                     TRUE ~ RECAUDO_VENCIDO),
         DIAS_DE_MORA_FIN = case_when(is.na(DIAS_DE_MORA_FIN) ~ 0,
                                      TRUE ~ DIAS_DE_MORA_FIN),
         EDAD_DE_MORA_FIN = case_when(is.na(EDAD_DE_MORA_FIN) ~ 0,
                                      TRUE ~ EDAD_DE_MORA_FIN))

ASIGNACION <- read_csv("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Insumos/Asiganciones Cierre/ASIGNACION.csv")

MLCARCRE_COMPLETO1 <- MLCARCRE_COMPLETO %>% 
  left_join(ASIGNACION, by = c("PEREVA" = "PERIODO","NITCLI"="CEDULA_ASOCIADO")) %>% 
  mutate(MORA_INI_HOMOLOGADA = case_when(EDAD_DE_MORA_INI >= 6 ~ 6,
                                         TRUE ~ EDAD_DE_MORA_INI),
         MORA_FIN_HOMOLOGADA = case_when(EDAD_DE_MORA_FIN >= 6 ~ 6,
                                         TRUE ~ EDAD_DE_MORA_FIN),
         Q_CONTENCIONES = case_when(EDAD_DE_MORA_FIN > 4 ~ 0,
                                    EDAD_DE_MORA_FIN <= EDAD_DE_MORA_INI ~ 1,
                                    TRUE ~ 0))

MLCARCRE_COMPLETO2 <- MLCARCRE_COMPLETO1 %>% 
  left_join(LINEA, by = "CODCPT") %>% 
  left_join(ESTADO, by = c("DESESTA_INI"="DESESTA")) %>% 
  left_join(ASIGNA, by = c("ACTOR_DE_GESTION_HOY"="ACTOR_DE_GESTION_HOY","SUBCAMPANA"="SUBCAMPANA")) %>% 
  mutate(ACTOR = case_when(is.na(ACTOR) ~ "OTROS",
                           TRUE ~ ACTOR)) %>% 
  mutate(CONTADOR = 1)


write_xlsx(MLCARCRE_COMPLETO2,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Resultado/MLCARCRE_COMPLETO.xlsx")


## 3. ---- Creacion Archivo  ---- Power BI ##

mlcarcre <- MLCARCRE_COMPLETO2

## Validación Duplicados ##

mlcarcre <- mlcarcre %>% arrange(NITCLI)

mlcarcre <- mlcarcre %>% mutate(Key = paste0(NITCLI,PEREVA))

mlcarcre <- mlcarcre %>%  mutate (Vali = if_else(Key == lead(Key),0,1))

## Filtramos Periodo Corriente ##

mlcarcre <- mlcarcre %>% filter(PEREVA == 202506) ## Cambiar periodo Corriente ##

## Leer Gestiones Mes - Coomeva ##

gestiones_coomeva <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Gestiones Provisionales/Gestiones_Mensuales.xlsx")

gestiones_coomeva <- gestiones_coomeva %>% mutate(periodo = 202506) ## Cambiar Periodo Corriente ##

gestiones_coomeva <- gestiones_coomeva %>% mutate(Key = paste0(identificacion_deudor,periodo))

## Qgestiones ##

gestiones_coomeva <- gestiones_coomeva %>%
  group_by(Key) %>%
  mutate(Qgestiones = n()) %>%
  ungroup()


## Homologacion Respuestas ##

gestiones_coomeva <- gestiones_coomeva %>% distinct()

Homologacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Homologacion_respuestas.xlsx")

gestiones_coomeva <- gestiones_coomeva %>% left_join(Homologacion %>% select(RESPUESTA,Contacto,`Contacto Directo`,Efectivo,Prioridad), by = c("Respuesta" = "RESPUESTA"))


## Qcontactos ##
gestiones_coomeva <- gestiones_coomeva %>% group_by(Key) %>% 
  mutate(Qcontactos = sum(`Contacto Directo`)) %>% 
  ungroup()


## Dejamos la de menor prioridad dentro de cada key (CCPeriodo)

Base_gestiones_reducida <- gestiones_coomeva %>%
  group_by(Key) %>%
  slice_min(Prioridad, with_ties = FALSE) %>%  # Toma la fila con menor prioridad
  ungroup()

mlcarcre <- mlcarcre %>% left_join(Base_gestiones_reducida %>% select(Key, Qgestiones, Contacto, `Contacto Directo`, Efectivo, Qcontactos), by = "Key")

## Evitamos Doble Contabilidad en gestiones (ASociados con 2 o mas creditos) ##

mlcarcre <- mlcarcre %>%
  mutate(
    Qgestiones = if_else(Vali == 0, 0, Qgestiones),
    Contacto = if_else(Vali == 0, 0, Contacto),
    `Contacto Directo` = if_else(Vali == 0, 0, `Contacto Directo`),
    Efectivo = if_else(Vali == 0, 0, Efectivo),
    Qcontactos = if_else(Vali == 0, 0, Qcontactos)
  )


## Creamos Columna Estado de Cartera ##

mlcarcre <- mlcarcre %>% mutate(EDAD_DE_MORA_INI = if_else(is.na(EDAD_DE_MORA_INI), 0, EDAD_DE_MORA_INI),
                                EDAD_DE_MORA_FIN = if_else(is.na(EDAD_DE_MORA_FIN), 0, EDAD_DE_MORA_FIN))


## Dejamos la edad maxima para asociados que tienen 2 o mas creditos ##
mlcarcre <- mlcarcre %>% group_by(Key) %>% 
  mutate(EDAD_DE_MORA_INI = max(EDAD_DE_MORA_INI),
         EDAD_DE_MORA_FIN = max(EDAD_DE_MORA_FIN)) %>% 
  ungroup()


## Generar Columna Estado_Cartera ##

# Extraer el día del corte#

mlcarcre <- mlcarcre %>%
  mutate(DIA_CORTE = as.numeric(sub(".*Al (\\d{2}) del Mes.*", "\\1", CORTE)))

## Matriz Corte ##

matriz_cortes <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cortes.xlsx")

# Hacer un join para agregar la columna de caída correspondiente al día de corte #
mlcarcre  <- mlcarcre %>%
  left_join(matriz_cortes, by = c("DIA_CORTE" = "Dia_Corte"))

# Obtener el día actual #
dia_actual <- as.numeric(format(Sys.Date(), "%d"))

# Definir si el corte ya pasó #
mlcarcre <- mlcarcre %>%
  mutate(CORTE_PASADO = ifelse(dia_actual >= Caida, TRUE, FALSE))

## Homologar la edad de mora ##

mlcarcre <- mlcarcre %>% 
  mutate(EDAD_DE_MORA_INI = if_else(EDAD_DE_MORA_INI > 7, 7, EDAD_DE_MORA_INI ))

mlcarcre <- mlcarcre %>% 
mutate(EDAD_DE_MORA_FIN = if_else(EDAD_DE_MORA_FIN > 7, 7, EDAD_DE_MORA_FIN))

## Crear la columna ESTADO CARTERA ##

mlcarcre <- mlcarcre %>%
  mutate(ESTADO_CARTERA = case_when(
    CORTE_PASADO & EDAD_DE_MORA_FIN == 0 ~ "NORMALIZA",
    CORTE_PASADO & EDAD_DE_MORA_FIN < EDAD_DE_MORA_INI ~ "MEJORA",
    CORTE_PASADO & EDAD_DE_MORA_FIN == EDAD_DE_MORA_INI ~ "MANTIENE",
    CORTE_PASADO & EDAD_DE_MORA_FIN > EDAD_DE_MORA_INI ~ "DETERIORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 0 & EDAD_DE_MORA_FIN == 0 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 1 & EDAD_DE_MORA_FIN == 1 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 1 & EDAD_DE_MORA_FIN == 0 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 2 & EDAD_DE_MORA_FIN == 2 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 2 & EDAD_DE_MORA_FIN == 1 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 2 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 3 & EDAD_DE_MORA_FIN == 3 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 3 & EDAD_DE_MORA_FIN == 2 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 3 & EDAD_DE_MORA_FIN == 1 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 3 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 4 & EDAD_DE_MORA_FIN == 4 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 4 & EDAD_DE_MORA_FIN == 3 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 4 & EDAD_DE_MORA_FIN == 2 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 4 & EDAD_DE_MORA_FIN == 1 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 4 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 5 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 4 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 3 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 2 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 1 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 5 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 6 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 5 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 4 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 3 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 2 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 1 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 6 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 7 ~ "POR VENCER",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 6 ~ "MANTIENE",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 5 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 4 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 3 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 2 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 1 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_INI == 7 & EDAD_DE_MORA_FIN == 0 ~ "MEJORA",
    !CORTE_PASADO & EDAD_DE_MORA_FIN > EDAD_DE_MORA_INI ~ "DETERIORA",
    TRUE ~ NA_character_
  ))

## Adicionamos el periodo corriente al archivo que alimenta BI ##

Bi <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Resultado/MLCARCRE_COMPLETO_JJVT.xlsx")


Bi <- Bi %>% filter(PEREVA != 202506) ## Cambiar periodo Corriente ##

mlcarcre <- mlcarcre %>% select(-Caida,-DIA_CORTE,-CORTE_PASADO)

mlcarcre <- mlcarcre %>% select(-Qcontactos, Qcontactos)

mlcarcre <- mlcarcre %>% mutate(CONTADOR = as.character(CONTADOR))

Bi <- Bi %>% bind_rows(mlcarcre)


## Sobreescribimos el archivo ##

write_xlsx(Bi,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Resultado/MLCARCRE_COMPLETO_JJVT.xlsx")

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

##Cargamos base de Gestiones ## 
Base_gestiones <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Gestiones Provisionales/Gestiones_Mensuales.xlsx")

## Cargamos Homologacion respuesta ## 

Homologacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Homologacion_respuestas.xlsx")

## Cruzamos por Respuesta ## 
Base_gestiones <- Base_gestiones %>% left_join(Homologacion %>% select(RESPUESTA,Prioridad,Contacto,`Contacto Directo`,Efectivo), by=c("Respuesta"="RESPUESTA"))

Base_gestiones <- Base_gestiones %>%
  distinct()

## Columna Promesa Cumplida ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/7. Julio/R10"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Seguimiento_diario <- read_excel(ultimo_archivo)

Seguimiento_diario <- Seguimiento_diario %>% mutate(`CEDULA ASOCIADO` = as.numeric(`CEDULA ASOCIADO`))

Base_gestiones <- Base_gestiones %>% mutate(identificacion_deudor= as.numeric(identificacion_deudor))

Base_gestiones <- Base_gestiones %>% left_join(Seguimiento_diario %>% select(`CEDULA ASOCIADO`,ESTADO_CARTERA), by=c("identificacion_deudor" ="CEDULA ASOCIADO"))

Base_gestiones <- Base_gestiones %>%
  mutate(
    Promesa_Cumplida = case_when(
      Efectivo == 1 & ESTADO_CARTERA %in% c("DETERIORA", "POR VENCER") ~ 0, # Efectivo = 1 y ESTADO_CARTERA en "Deteriora" o "Por vencer"
      Efectivo == 1 ~ 1, # Efectivo = 1 pero ESTADO_CARTERA no está en "Deteriora" o "Por vencer"
      TRUE ~ 0 # Para todos los demás casos
    )
  )

## Traerme Edad de Mora del agente ##

Planta <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/PLANTA JULIO.xlsx")

Base_gestiones<- Base_gestiones %>% left_join(Planta %>% select(USUARIO,CEDULA,`EDAD DE MORA`,`NOMBRE AGENTE`), by=c("usuario" = "USUARIO"))

## Nos traemos Recaudo Vencido Por Agente ## 

Base_gestiones <- Base_gestiones %>% left_join(Seguimiento_diario %>% select(`CEDULA ASOCIADO`,`VALOR RECAUDO VENCIDO`),by=c("identificacion_deudor"= "CEDULA ASOCIADO"))

## SUBCAMPAÑA HOMOLOGADA ##

Seguimiento_diario <- Seguimiento_diario %>%
  mutate(SUBCAMPAÑA_HOMOLOGADA = case_when(
    SUBCAMPAÑA %in% c("CSSMORA1CORTE5", "CSSMORA1CORTE10", "CSSMORA1CORTE15", 
                      "CSSMORA1CORTE20", "CSSMORA1CORTE25", "CSSMORA1CORTE30") ~ "Mora 1",
    SUBCAMPAÑA == "CSS MORA 2" ~ "Mora 2",
    SUBCAMPAÑA == "CSS MORA 3" ~ "Mora 3",
    SUBCAMPAÑA == "CSS MORA 4" ~ "Mora 4",
    SUBCAMPAÑA == "REG BOGOTA" ~ "Regional Bogota",
    SUBCAMPAÑA == "REG CALI" ~ "Regional Cali",
    SUBCAMPAÑA == "REG CARIBE" ~ "Regional Caribe",
    SUBCAMPAÑA == "REG EJECAFETERO" ~ "Regional Eje",
    SUBCAMPAÑA == "REG MEDELLIN" ~ "Regional Medellin",
    SUBCAMPAÑA == "REG PALMIRA" ~ "Regional Palmira",
    SUBCAMPAÑA %in% c("CSSALTAPRIORID", "CSSBAJAPRIORID") ~ "INACTIVOS",
    SUBCAMPAÑA == "BANCOOMEVA_U" ~ "BANCOOMEVA",
    SUBCAMPAÑA %in% c("CARTERA CASTIGO","MORA >= 4 CR","MORA 1 CR","MORA 2 - 3 CR") ~ "Creditos",
    TRUE ~ SUBCAMPAÑA 
  ))

Asignacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Asignacion Julio 2025.xlsx")

Asignacion <- Asignacion %>% mutate(IDENTIFICACION = as.double(IDENTIFICACION))

Seguimiento_diario <- Seguimiento_diario %>% left_join(Asignacion %>% select(IDENTIFICACION, USUARIO), by=c("CEDULA ASOCIADO" = "IDENTIFICACION"))

Seguimiento_diario <- Seguimiento_diario %>%
  rename(USUARIO_GESTION = USUARIO)

Seguimiento_diario <- Seguimiento_diario %>%
  mutate(USUARIO_GESTION = toupper(USUARIO_GESTION))

Seguimiento_diario <- Seguimiento_diario %>% left_join(Planta %>% select(USUARIO,`NOMBRE AGENTE`), by = c("USUARIO_GESTION" = "USUARIO"))

Seguimiento_diario <- Seguimiento_diario %>% rename(NOMBRE_HOMOLOGADO = `NOMBRE AGENTE`)

columnas <- c(
  "CEDULA ASOCIADO", "NOMBRE ASOCIADO", "REGIONAL", "ZONA", "TIPO CLIENTE", 
  "DESCRIPCIÓN DEL CORTE", "DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY", 
  "EDAD CARTERA INICIAL", "EDAD CARTERA HOY", "VALOR RECAUDO VENCIDO", 
  "VALOR VENCIDO PERIODO", "SUBCAMPAÑA", "DÉBITO AUTOMÁTICO", 
  "ANTIGUEDAD DEL ASOCIADO EN COOMEVA", "EDAD ASOCIADO", "TELÉFONO RESIDENCIAL", 
  "TELÉFONO CORRESPONDENCIA", "TELÉFONO FAMILIAR", "TELÉFONO CELULAR", 
  "TELÉFONO  OFICINA", "E-MAIL", "USUARIO ASIGNADO", "SEGMENTO", 
  "USUARIO  AUXIILIAR DE GESTIÓN", "VALOR VENCIDO HOY", "ESTADO_CARTERA", 
  "PRIORIDAD_MONTO", "PRIORIDAD_RIESGO", "PRIORIDAD_GESTION", 
  "fecha_ultima_gestion", "fecha_promesa", "Estado_compromiso", "Acción", 
  "Respuesta", "motivo_nopago", "Contacto", "Contacto Directo", "Efectivo", 
  "Qgestiones", "Promesa_Cumplida", "EDAD_CARTERA_HOMOLOGADA", "Revesta", 
  "ACR", "Alivio_financiero", "Gestion_Reciente", "Saldo_menor", 
  "gestion_base", "CONCEPTO MAS VENCIDO", "SUBCAMPAÑA_HOMOLOGADA", 
  "USUARIO_GESTION", "NOMBRE_HOMOLOGADO"
)

Seguimiento_diario <- Seguimiento_diario %>% select(columnas)


## Escribir Base Productividad ##
write_xlsx(Base_gestiones,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Resultado/Bases/Productividad/Base_Productividad_Julio_2025.xlsx")

## Escribir Base Recaudo ## 
write_xlsx(Seguimiento_diario,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Resultado/Bases/Recaudo/Base_Recaudo_Julio_2025.xlsx")
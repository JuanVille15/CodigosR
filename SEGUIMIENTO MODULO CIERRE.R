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

ruta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

data <- data %>%
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))

data <- data %>%
  filter(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo","Receso"))

Subcampaña <- c("CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                "CSSMORA1CORTE30", "CSSMORA1CORTE5", "REG BOGOTA", "REG CALI", "REG CARIBE", "REG EJECAFETERO",
                "REG MEDELLIN", "REG PALMIRA","CSSCUOTA0HASTA6")

data <- data %>%
  filter(`SUBCAMPAÑA` %in% Subcampaña )


## FILTRAMOS LAS COLUMNAS QUE NECESITAMOS ## 

# Seleccionar las columnas deseadas #

columnas_deseadas <- c("CEDULA ASOCIADO", "NOMBRE ASOCIADO", "REGIONAL", "ZONA", 
                       "TIPO CLIENTE", "DESCRIPCIÓN DEL CORTE", "DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY", 
                       "EDAD CARTERA INICIAL", "EDAD CARTERA HOY", "VALOR RECAUDO VENCIDO", 
                       "VALOR VENCIDO PERIODO", "SUBCAMPAÑA", "DÉBITO AUTOMÁTICO", 
                       "ANTIGUEDAD DEL ASOCIADO EN COOMEVA", "EDAD ASOCIADO", "TELÉFONO RESIDENCIAL", 
                       "TELÉFONO CORRESPONDENCIA", "TELÉFONO FAMILIAR", "TELÉFONO CELULAR", 
                       "TELÉFONO  OFICINA", "E-MAIL", "USUARIO ASIGNADO", "SEGMENTO", 
                       "USUARIO  AUXIILIAR DE GESTIÓN","VALOR VENCIDO HOY","CONCEPTO MAS VENCIDO")

# Filtrar las columnas#

R_10 <- data %>% select(all_of(columnas_deseadas))

## Crear la columna ESTADO CARTERA ##

R_10 <- R_10 %>%  mutate(ESTADO_CARTERA = case_when(
  `EDAD CARTERA HOY` == 0 ~ "NORMALIZA",
  `EDAD CARTERA HOY` < `EDAD CARTERA INICIAL` ~ "MEJORA",
  `EDAD CARTERA HOY` == `EDAD CARTERA INICIAL` ~ "MANTIENE",
  `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
  TRUE ~ NA_character_
))

## CREAR COLUMNA PRIORIDAD MONTO - CAMBIAR VALORES CUANDO SE NECESITE ## 

MONTO_BAJO <- 149767.5
MONTO_MEDIO <- 222666
MONTO_ALTO <- 416235 


R_10 <- R_10 %>%
  mutate(PRIORIDAD_MONTO = case_when(`VALOR VENCIDO PERIODO` <= MONTO_BAJO ~ "Monto Bajo",
                                     `VALOR VENCIDO PERIODO` > MONTO_BAJO & `VALOR VENCIDO PERIODO`<MONTO_ALTO ~ "Monto Medio",
                                     `VALOR VENCIDO PERIODO`>= MONTO_ALTO ~ "Monto Alto" ))
## CAMBIAR SEGMENTACION CADA MES## 

Segmentacion_mensual <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Matriz_riesgo_monto Nov2024.xlsx")

## CREAR COLUMNA PRIORIDAD RIESGO ##
R_10 <- R_10 %>% left_join(Segmentacion_mensual %>% select(Segmentacion,Riesgo), by=c("SEGMENTO" = "Segmentacion"))


R_10 <- R_10 %>% rename(PRIORIDAD_RIESGO = Riesgo) %>%
mutate(PRIORIDAD_RIESGO = ifelse(is.na(PRIORIDAD_RIESGO),"Riesgo bajo", PRIORIDAD_RIESGO))

## CREAR COLUMNA PRIORIDAD GESTION ## 

R_10 <- R_10 %>%
  mutate(PRIORIDAD_GESTION = case_when(
    PRIORIDAD_RIESGO == "Riesgo Alto" & PRIORIDAD_MONTO %in% c("Monto Alto", "Monto Medio") ~ "Prioridad 1",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Alto" ~ "Prioridad 1",
    PRIORIDAD_RIESGO == "Riesgo bajo" & PRIORIDAD_MONTO == "Monto Alto" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Medio" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo Alto" & PRIORIDAD_MONTO == "Monto Bajo" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo bajo" & PRIORIDAD_MONTO %in% c("Monto Medio", "Monto Bajo") ~ "Prioridad 3",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Bajo" ~ "Prioridad 3",
    TRUE ~ NA_character_
  ))

## Transformaciones a la base de gestiones ## 

##Cargamos base de Gestiones ## 
Base_gestiones <- read_csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506.csv")

  Base_gestiones <- Base_gestiones %>%
  distinct()

## Cargamos Homologacion respuesta ## 

Homologacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Homologacion_respuestas.xlsx")

## Columna Proridad ## 

Base_gestiones <- Base_gestiones %>% left_join(Homologacion %>% select(RESPUESTA,Prioridad,Contacto,`Contacto Directo`,Efectivo), by=c("Respuesta"="RESPUESTA"))

Base_gestiones <- Base_gestiones %>%
  arrange(
    identificacion_deudor,  # Ordenar de menor a mayor por identificación
    Prioridad,              # Ordenar de menor a mayor por prioridad
    desc(fecha_gestion)     # Ordenar de mayor a menor por fecha de gestión
  )

Base_gestiones <- Base_gestiones %>%
  group_by(identificacion_deudor) %>%               # Agrupa por deudor
  mutate(fecha_ultima_gestion = max(fecha_gestion)) %>% # Encuentra la fecha máxima por grupo
  ungroup()  

Fecha_hoy <- format(Sys.Date(), "%d-%m-%Y")

Base_gestiones <- Base_gestiones %>%
  mutate(
    Estado_compromiso = case_when(
      is.na(fecha_promesa) ~ "Sin promesa",                     # Si fecha_promesa está vacía
      fecha_promesa < Fecha_hoy ~ "Promesa rota",              # Si fecha_promesa es menor a hoy
      fecha_promesa == Fecha_hoy ~ "Promesa Hoy",              # Si fecha_promesa es igual a hoy
      fecha_promesa > Fecha_hoy ~ "Promesa pendiente"          # Si fecha_promesa es mayor a hoy
    )
  )



Base_gestiones <- Base_gestiones %>%
  group_by(identificacion_deudor) %>%            # Agrupa por identificacion_deudor
  mutate(Qgestiones = n()) %>%                   # Cuenta las filas en cada grupo
  ungroup()                                      # Elimina la agrupación


## Cruzar las bases x prioridad ##

Base_gestiones_reducida <- Base_gestiones %>%
  group_by(identificacion_deudor) %>%
  slice_min(Prioridad, with_ties = FALSE) %>%  # Toma la fila con menor prioridad
  ungroup()

R_10 <- R_10 %>%
  mutate(`CEDULA ASOCIADO` = as.numeric(`CEDULA ASOCIADO`))

Base_gestiones_reducida <- Base_gestiones_reducida %>%
  mutate(`identificacion_deudor` = as.numeric(`identificacion_deudor`))

R_10 <- R_10 %>%
  left_join(
    Base_gestiones_reducida %>%
      select(
        identificacion_deudor,
        fecha_ultima_gestion, 
        fecha_promesa, 
        Estado_compromiso, 
        Acción, 
        Respuesta, 
        motivo_nopago, 
        Contacto, 
        `Contacto Directo`, 
        Efectivo,
        Qgestiones
        ),
    by = c("CEDULA ASOCIADO" = "identificacion_deudor"
  ))

R_10 <- R_10 %>%
  mutate(Respuesta = ifelse(is.na(Respuesta) | Respuesta == "", "SIN_GESTION", Respuesta))

R_10 <- R_10 %>%
  mutate(
    Promesa_Cumplida = case_when(
      Efectivo == 1 & ESTADO_CARTERA %in% c("DETERIORA", "POR VENCER") ~ 0, # Efectivo = 1 y ESTADO_CARTERA en "Deteriora" o "Por vencer"
      Efectivo == 1 ~ 1, # Efectivo = 1 pero ESTADO_CARTERA no está en "Deteriora" o "Por vencer"
      TRUE ~ 0 # Para todos los demás casos
    )
  )

R_10 <- R_10 %>%
  mutate(EDAD_CARTERA_HOMOLOGADA = if_else(`EDAD CARTERA INICIAL` <= 4, 
                                           `EDAD CARTERA INICIAL`, 
                                           4))
### ALTERNATIVAS ### 

## REVESTA ##

ruta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Revesta"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Revesta <- read_excel(ultimo_archivo)

R_10 <- R_10 %>%
  mutate(Revesta = if_else(`CEDULA ASOCIADO` %in% Revesta$Cedula, "SI", "NO"))


## Anulacion ACR ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Anulacion ACR/Base Ofrecimiento"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
ACR <- read_excel(ultimo_archivo)

R_10 <- R_10 %>%
  mutate(ACR = if_else(`CEDULA ASOCIADO` %in% ACR$ASO_IDENTIFICACION, "SI", "NO"))

## Alivio Financiero ## 

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/2025"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Alivio_Financiero <- read_excel(ultimo_archivo,sheet ="DATA SIMULADOR")

R_10 <- R_10 %>%
  mutate(Alivio_financiero = if_else(`CEDULA ASOCIADO` %in% Alivio_Financiero$ASO_IDENTIFICACION, "SI", "NO"))

R_10 <- R_10 %>%
  distinct()

## Columna Gestion Reciente ##

R_10 <- R_10 %>% mutate(fecha_ultima_gestion = as.Date(fecha_ultima_gestion, format ="%d/%m/%Y" ))

R_10 <- R_10 %>%
  mutate(Gestion_Reciente = if_else(
    is.na(fecha_ultima_gestion) | 
      (Sys.Date() - fecha_ultima_gestion) > 3, 
    "SIN_GESTION", 
    "GESTIONADO"
  ))

## Columna Saldos Menores ##

R_10 <- R_10 %>%
  mutate(
    Saldo_menor = case_when(
      `VALOR VENCIDO HOY` <= 3000 & `VALOR VENCIDO HOY` > 0 ~ "SI", 
      `VALOR VENCIDO HOY` > 3000 ~ "NO",                        
      TRUE ~ NA_character_                                    
    )
)

## Gestión de Base ## 

R_10 <- R_10 %>% mutate(gestion_base = if_else(Respuesta== "SIN_GESTION",0,1))

R_10 <- R_10 %>%
  relocate(`CONCEPTO MAS VENCIDO`, .after = last_col())


## Homologación Actor de Gestión ##

R_10 <- R_10 %>% mutate(ACTOR_GESTION_HOMOLOGADO = case_when(SUBCAMPAÑA %in% c("CSSMORA1CORTE5","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20",
                                                                             "CSSMORA1CORTE25","CSSMORA1CORTE30","CSS MORA 2","CSS MORA 3","CSS MORA 4",
                                                                             "CSSCUOTA0HASTA6","REG BOGOTA","REG CALI","REG MEDELLIN","REG EJECAFETERO","REG PALMIRA","REG CARIBE") ~ "Moras Tempranas",
                                                           SUBCAMPAÑA == "BANCOOMEVA_U" ~ "Bancoomeva",
                                                           SUBCAMPAÑA %in% c("CARTERA CASTIGO","MORA >= 4 CR","MORA 1 CR","MORA 2 - 3 CR") ~ "Creditos",
                                                           SUBCAMPAÑA %in% c("CSSALTAPRIORID", "CSSBAJAPRIORID") ~ "Inactivos",
                                                           TRUE ~ SUBCAMPAÑA ))

## Escribimos el R_10 del dia ##

ruta_archivo <- paste0("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/6. Junio/R10/R10-CIERRE-", Fecha_hoy,".xlsx")
write_xlsx(R_10, ruta_archivo)

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
library(zoo)
library(stringr)


## Archivo ejemplo ##

r10_Ejemplo <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Reporte10_Cierre_Enero.xlsx")

ruta_R10 <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024"

ruta_gestiones <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Gestiones" 

# Lista de meses a procesar
meses <- c("Octubre", "Noviembre", "Diciembre")


for (mes in meses) {
  archivo_gestiones <- file.path(ruta_gestiones, paste0("Gestiones_", mes, ".xlsx"))
  archivo_cierre <- file.path(ruta_R10, paste0("Reporte10_Cierre_", mes, ".xlsx"))
  # Cargar datos  
  gestiones <- read_excel(archivo_gestiones)
  cierre <- read_excel(archivo_cierre)
  
  ## Elegir columnas ## 
  
  cierre <- cierre[, 1:89]
  
  ## Asegurar que todos tengan la columnas bien nombradas ##
  
 colnames(cierre) <- colnames(r10_Ejemplo)
 
 ## Desde aqui va el codigo del seguimiento ##

cierre <- cierre %>%
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))

cierre <- cierre %>%
  filter(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo","Receso"))

Subcampaña <- c("CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                 "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                 "CSSMORA1CORTE30", "CSSMORA1CORTE5", "REG BOGOTA", "REG CALI", "REG CARIBE", "REG EJECAFETERO",
                 "REG MEDELLIN", "REG PALMIRA","CSSCUOTA0HASTA6","CARTERA CASTIGO","CSSALTAPRIORID","CSSBAJAPRIORID","BANCOOMEVA_U","MORA >= 4 CR",
                 "MORA 1 CR","MORA 2 - 3 CR")

cierre <- cierre %>%
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

R_10 <- cierre %>% select(all_of(columnas_deseadas))

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


R_10 <- R_10 %>% rename(PRIORIDAD_RIESGO= Riesgo) %>%
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


gestiones <- gestiones %>%
  distinct()

## Cargamos Homologacion respuesta ## 

Homologacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Homologacion_respuestas.xlsx")

## Columna Proridad ## 

gestiones <- gestiones %>% left_join(Homologacion %>% select(RESPUESTA,Prioridad,Contacto,`Contacto Directo`,Efectivo), by=c("Respuesta"="RESPUESTA"))

gestiones <- gestiones %>%
  arrange(
    identificacion_deudor,  # Ordenar de menor a mayor por identificación
    Prioridad,              # Ordenar de menor a mayor por prioridad
    desc(fecha_gestion)     # Ordenar de mayor a menor por fecha de gestión
  )

gestiones <- gestiones %>%
  group_by(identificacion_deudor) %>%               # Agrupa por deudor
  mutate(fecha_ultima_gestion = max(fecha_gestion)) %>% # Encuentra la fecha máxima por grupo
  ungroup()  

Fecha_hoy <- format(Sys.Date(), "%d-%m-%Y")

gestiones <- gestiones %>%
  mutate(
    Estado_compromiso = case_when(
      is.na(fecha_promesa) ~ "Sin promesa",                     # Si fecha_promesa está vacía
      fecha_promesa < Fecha_hoy ~ "Promesa rota",              # Si fecha_promesa es menor a hoy
      fecha_promesa == Fecha_hoy ~ "Promesa Hoy",              # Si fecha_promesa es igual a hoy
      fecha_promesa > Fecha_hoy ~ "Promesa pendiente"          # Si fecha_promesa es mayor a hoy
    )
  )



gestiones <- gestiones %>%
  group_by(identificacion_deudor) %>%            # Agrupa por identificacion_deudor
  mutate(Qgestiones = n()) %>%                   # Cuenta las filas en cada grupo
  ungroup()                                      # Elimina la agrupación


## Cruzar las bases x prioridad ##

Base_gestiones_reducida <- gestiones %>%
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

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/2024"
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
                                                                             "CSSCUOTA0HASTA6","REG BOGOTA","REG CALI","REG MEDELLIN","REG EJECAFETERO","REG PALMIRA","REG CARIBE", "CSS MORA 5","CSS MORA 6","CSS MORA 7") ~ "Moras Tempranas",
                                                           SUBCAMPAÑA == "BANCOOMEVA_U" ~ "Bancoomeva",
                                                           SUBCAMPAÑA %in% c("CARTERA CASTIGO","MORA >= 4 CR","MORA 1 CR","MORA 2 - 3 CR") ~ "Creditos",
                                                           SUBCAMPAÑA %in% c("CSSALTAPRIORID", "CSSBAJAPRIORID") ~ "Inactivos",
                                                           TRUE ~ SUBCAMPAÑA ))

## Escribimos el R_10 del dia ##

write_xlsx(R_10,glue("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Resultado/Seguimiento_{mes}_2024.xlsx"))
}
  
  
  
  
## Unir los archivos en un data frame ##

Carpeta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Resultado"

# Obtener la lista de archivos Excel en la carpeta

archivos <- list.files(path = Carpeta, pattern = "\\.xlsx$", full.names = TRUE)


## Lista de datos ##
lista_datos <- list()

for (archivo in archivos) {
  # Obtener el nombre del archivo sin la ruta
  nombre_archivo <- basename(archivo)
  
  # Limpiar el nombre del archivo para obtener solo "Mes_2024"
  nombre_limpio <- gsub("Seguimiento_|\\.xlsx", "", nombre_archivo)
  
  # Extraer mes y año
  partes <- str_split(nombre_limpio, "_")[[1]]
  mes <- partes[1]
  anio <- partes[2]
  
  # Convertir a formato yearmon (Mes Año como fecha sin día)
  fecha_real <- as.yearmon(paste(mes, anio), "%B %Y")
  
  # Leer el archivo Excel 
  datos <- read_excel(archivo)
  
  # Agregar la columna "Periodo" con formato de fecha
  datos <- datos %>% mutate(Periodo = fecha_real)
  
  # Agregar el dataframe a la lista
  lista_datos[[nombre_limpio]] <- datos
}

df_final <- bind_rows(lista_datos)

df_final <- df_final %>% select(Periodo,everything())

## Seleccionar Columnas ##

columnas <- c("Periodo",
              "CEDULA ASOCIADO",
              "NOMBRE ASOCIADO",
              "REGIONAL",
              "DESCRIPCIÓN DEL CORTE",
              "DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY",
              "EDAD CARTERA INICIAL",
              "EDAD CARTERA HOY",
              "VALOR RECAUDO VENCIDO",
              "VALOR VENCIDO PERIODO",
              "SUBCAMPAÑA",
              "VALOR VENCIDO HOY",
              "ESTADO_CARTERA",
              "PRIORIDAD_MONTO",
              "PRIORIDAD_RIESGO",
              "PRIORIDAD_GESTION",
              "fecha_ultima_gestion",
              "fecha_promesa",
              "Estado_compromiso",
              "Acción",
              "Respuesta",
              "motivo_nopago",
              "Contacto",
              "Contacto Directo",
              "Efectivo",
              "Qgestiones",
              "Promesa_Cumplida",
              "EDAD_CARTERA_HOMOLOGADA",
              "Gestion_Reciente",
              "Saldo_menor",
              "gestion_base",
              "ACTOR_GESTION_HOMOLOGADO"
              )

df_final <- df_final %>% select(columnas)

write.csv(df_final,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Insumo_Tablero/insumo.csv")

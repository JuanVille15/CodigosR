library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)
library(conflicted)
library(dbplyr)
library(lubridate)
library(janitor)
library(readr)
library(Microsoft365R)
library(blastula)
library(janitor)

conflicts_prefer(dplyr::filter)

## Moras Altas## -----

## Leemos la ruta ##
Archivos <- list.files("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cierres GH/Mora 3 y 4", full.names = TRUE)

## Leer todos los archivos ##

Moras_Altas <- map_df(Archivos,function(Archivo){
    read_excel(Archivo, sheet = "LIQUIDACIÓN AGENTES") %>% 
                    mutate(Periodo = substr(basename(Archivo),7,12))
    })

Moras_Altas <- Moras_Altas %>% select(Periodo,Cedula,Nombre,Mora,`CUMPL CONTENCIÓN DE RODAMIENTO`,`TOTAL PUNTOS MATRIZ`) %>% 
                                filter(Cedula != is.na(Cedula))

Moras_Altas <- Moras_Altas %>% mutate(`TOTAL PUNTOS MATRIZ` = (`TOTAL PUNTOS MATRIZ`)/100,
                                      Mora = str_to_title(Mora),
                                      Nombre = str_to_title(Nombre))


Moras_Altas <- Moras_Altas %>% mutate(CUMP_INDICE_RECAUDO = "")

## Regionales ## ----

Archivos <- list.files("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cierres GH/Regionales", full.names = TRUE)


Regionales <- map_df(Archivos,function(Archivo){
    read_excel(Archivo, sheet = "LIQUIDACIÓN") %>% 
        mutate(Periodo = substr(basename(Archivo),4,9))
})

Regionales <- Regionales %>% select(Periodo,CEDULA,NOMBRE,`CUMPL CONTENCIÓN DE RODAMIENTO`, `CUMPL ÍNDICE DE RECAUDO`,`CUMPL METAS PROMESAS`,`CUMPL PROMESAS CUMPLIDAS`,`CUMPL PERSUACIÓN`, `PUNTOS TOTAL`)

Regionales <- Regionales %>% mutate(Mora = "Regionales")


Regionales <- Regionales %>% mutate(`PUNTOS TOTAL` = `PUNTOS TOTAL`/100,
                                    NOMBRE = str_to_title(NOMBRE))

## Renombrar Columnas ##

Moras_Altas <- Moras_Altas %>% rename(Nombre_Agente = Nombre,
                                      CUMP_INDICE_CONTENCION = `CUMPL CONTENCIÓN DE RODAMIENTO`,
                                      CUMP_TOTAL = `TOTAL PUNTOS MATRIZ`)

Moras_Altas <- Moras_Altas %>% select(Periodo,Cedula,Nombre_Agente,Mora,CUMP_INDICE_RECAUDO,CUMP_INDICE_CONTENCION,CUMP_TOTAL)

Regionales <- Regionales %>% select(Periodo,CEDULA,NOMBRE,Mora,`CUMPL ÍNDICE DE RECAUDO`,`CUMPL CONTENCIÓN DE RODAMIENTO`,`CUMPL METAS PROMESAS`,`CUMPL PROMESAS CUMPLIDAS`,`CUMPL PERSUACIÓN`,`PUNTOS TOTAL`)

Regionales <- Regionales %>% rename(Cedula = CEDULA,
                                    Nombre_Agente = NOMBRE,
                                    CUMP_INDICE_RECAUDO = `CUMPL ÍNDICE DE RECAUDO`,
                                    CUMP_INDICE_CONTENCION = `CUMPL CONTENCIÓN DE RODAMIENTO`,
                                    CUMP_TOTAL = `PUNTOS TOTAL`,
                                    CUMP_PROMESAS = `CUMPL METAS PROMESAS`,
                                    CUMP_PROMESAS_CUMPLIDAS = `CUMPL PROMESAS CUMPLIDAS`,
                                    CUMP_PERSUACION = `CUMPL PERSUACIÓN`)

## Mora 0 y 1 ## ----

## Leemos la ruta ##
Archivos <- list.files("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cierres GH/Mora 1 y  2", full.names = TRUE)

## Leer todos los archivos ##

Moras_Bajas <- map_df(Archivos,function(Archivo){
    read_excel(Archivo, sheet = "LIQUIDACIÓN AGENTES", col_types = "text") %>% 
        mutate(Periodo = substr(basename(Archivo),8,13))
})

Moras_Bajas <- Moras_Bajas %>% select(Periodo, Cedula,Nombre,Mora,`CUMPL ÍNDICE DE RECAUDO`,`CUMPL CONTENCIÓN DE RODAMIENTO`,`CUMPL METAS PROMESAS (INDIVIDUAL)`,`PROMESAS CUMPLIDAS (INDIVIDUAL)`,`CUMPL PERSUACIÓN (INDIVIDUAL)`,`TOTAL PUNTOS MATRIZ`)

Moras_Bajas <- Moras_Bajas %>% mutate(Cedula = as.double(Cedula),
                                      `CUMPL ÍNDICE DE RECAUDO` = as.double(`CUMPL ÍNDICE DE RECAUDO`),
                                      `CUMPL CONTENCIÓN DE RODAMIENTO` = as.double(`CUMPL CONTENCIÓN DE RODAMIENTO`),
                                      `CUMPL METAS PROMESAS (INDIVIDUAL)` = as.double(`CUMPL METAS PROMESAS (INDIVIDUAL)`),
                                      `PROMESAS CUMPLIDAS (INDIVIDUAL)` = as.double(`PROMESAS CUMPLIDAS (INDIVIDUAL)`),
                                      `CUMPL PERSUACIÓN (INDIVIDUAL)` = as.double(`CUMPL PERSUACIÓN (INDIVIDUAL)`),
                                      `TOTAL PUNTOS MATRIZ` = as.double(`TOTAL PUNTOS MATRIZ`))


Moras_Bajas <- Moras_Bajas %>% rename(Nombre_Agente = Nombre,
                                      CUMP_INDICE_RECAUDO = `CUMPL ÍNDICE DE RECAUDO`,
                                      CUMP_INDICE_CONTENCION = `CUMPL CONTENCIÓN DE RODAMIENTO`,
                                      CUMP_PROMESAS = `CUMPL METAS PROMESAS (INDIVIDUAL)`,
                                      CUMP_PROMESAS_CUMPLIDAS = `PROMESAS CUMPLIDAS (INDIVIDUAL)`,
                                      CUMP_PERSUACION = `CUMPL PERSUACIÓN (INDIVIDUAL)`,
                                      CUMP_TOTAL = `TOTAL PUNTOS MATRIZ`)

Moras_Bajas <- Moras_Bajas %>% mutate(Mora = str_to_title(Mora),
                                      Nombre_Agente = str_to_title(Nombre_Agente),
                                      CUMP_TOTAL = CUMP_TOTAL/100)

Moras_Altas <- Moras_Altas %>% mutate(CUMP_PROMESAS = "",
                                      CUMP_PROMESAS_CUMPLIDAS = "",
                                      CUMP_PERSUACION = "")

Moras_Altas <- Moras_Altas %>% select(colnames(Moras_Bajas))

Moras_Altas <- Moras_Altas %>% mutate(CUMP_PROMESAS = as.double(CUMP_PROMESAS),
                                      CUMP_PROMESAS_CUMPLIDAS = as.double(CUMP_PROMESAS_CUMPLIDAS),
                                      CUMP_PERSUACION = as.double(CUMP_PERSUACION),
                                      CUMP_INDICE_RECAUDO = as.double(CUMP_INDICE_RECAUDO))

df_total <- Moras_Bajas %>% bind_rows(Moras_Altas)

df_total <- df_total %>% bind_rows(Regionales)

df_total <- df_total %>% arrange(Periodo)

df_total <- df_total %>% mutate(cumple = if_else(CUMP_TOTAL >= 1.0,1,0))

PlantaActiva <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cierres GH/PlantaActiva.xlsx")
    
df_total <- df_total %>% mutate(EstadoColaborador = if_else(Cedula %in% PlantaActiva$CEDULA, "Si","No"))

df_total <- df_total %>% group_by(Cedula) %>% 
                    mutate(QPeriodos = n(),
                           QCump = sum(cumple),
                           Promedio_Cump = mean(CUMP_TOTAL)) %>% 
                                    ungroup()

df_total <- df_total %>% mutate(Rango_Cumplimiento = case_when(Promedio_Cump >= 1.0 ~ "Top Performance",
                                                               Promedio_Cump >= 0.9 ~ "Estandar",
                                                               Promedio_Cump < 0.9 ~ "Bajo Cumplimiento",
                                                              TRUE ~ "" ))

write_xlsx(df_total,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cierres GH/Consol.xlsx")
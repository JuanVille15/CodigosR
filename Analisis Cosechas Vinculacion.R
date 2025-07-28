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
library(stringr)


### ANALISIS COSECHAS DE VINCULACIÓN ###

## Leemos Consolidado Vinculación ##

Consol <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Consolidado Vinculaciones 2024.xlsx")

## Creamos Columna Periodo ##

Consol <- Consol %>%
  mutate(Periodo = format(as.Date(`Fecha Vinculación`, format = "%d/%m/%Y"), "%Y%m"))


## Estado Hoy ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KyCo_090625*")


Stm_Hoy <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
     aso.aso_identificacion,
     aso.aso_fecha_nacimiento,
     reg.reg_nombre,
     zon.zon_nombre,
     stm.estm_nombre,
     aso.aso_fecha_cambio_estado_mult,
     aso.aso_fecha_ingreso
     FROM grc.gcc_asociado aso
     LEFT OUTER JOIN grc.GCC_ESTADOS_MULTIACTIVA  stm
        ON aso.estm_codigo = stm.estm_codigo
     LEFT OUTER JOIN grc.GCC_REGIONAL reg
        ON TRIM(aso.reg_codigo) = TRIM(reg.reg_codigo)
     LEFT OUTER JOIN grc.GCC_ZONA zon
        ON TRIM(aso.zon_codigo) = TRIM(zon.zon_codigo);"))                            

Consol <- Consol %>% left_join(Stm_Hoy %>% select(ASO_IDENTIFICACION,ESTM_NOMBRE), by = c("Identificación Asociado" = "ASO_IDENTIFICACION"))

Consol <- Consol %>% rename(Stm_Hoy = ESTM_NOMBRE)


Consol <- Consol %>% mutate(Stm_Hoy = case_when(Stm_Hoy %in% c("Retirado Normal","Retirado Cobranza Interna","Suspendido Cobranza Interna",
                                                                 "Excluido Cobranza Interna", "Fallecido Normal","Expulsion de la Cooperativa",
                                                                 "Suspendido por Fallecimiento", "Suspendido Normal") ~ "Otros Retiros",
                                                  TRUE ~ Stm_Hoy))

## Leemos archivo consolidado de r10 cierre ##

files <- list.files(path = "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/R10Cierre",full.names = TRUE)

r10 <- map_dfr(files, function(file){read_excel(file, col_types = "text") %>% 
                            mutate(Periodo = substr(basename(file),1, 6))
})

r10 <- r10 %>% filter(`TIPO CLIENTE` %in% c("Solo Asociado", "Mixto (Asociado/Cliente)"))

r10 <- r10 %>% select(`CEDULA ASOCIADO`,Periodo,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`)

## Cruzamos los valores ## 

Consol <- Consol %>% distinct(`Identificación Asociado`, Periodo, .keep_all = TRUE)

r10 <- r10 %>% distinct(`CEDULA ASOCIADO`,Periodo,.keep_all = TRUE)

r10 <- r10 %>% mutate(`CEDULA ASOCIADO` = as.double(`CEDULA ASOCIADO`))

Consol <- Consol %>% inner_join(r10 %>% select(`CEDULA ASOCIADO`,Periodo,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`), by = c("Identificación Asociado" = "CEDULA ASOCIADO"))

Consol <- Consol %>% rename(Periodo_Vinc = Periodo.x,
                           Periodo_Estado = Periodo.y)


Consol1 <- Consol %>% pivot_wider(id_cols = c(`Identificación Asociado`,`Nombre Asociado`,`Tipo Persona`,`Fecha Vinculación`,`Regional Asociado`,`Zona Asociado`,`Cod. Oficina Asociado`,`Identificación Asociado`,
                                              `Oficina Asociado`,`Tipo Perfil`,Perfil,`Identificacion Asesor`,`Nombre Asesor`,Periodo_Vinc,Stm_Hoy),
                                 
                                 names_from = Periodo_Estado,
                                 values_from = `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,
                                 names_prefix = "stm_"
                                 )


Consol1 <- Consol1 %>% relocate(`stm_202401`,`stm_202402`,`stm_202403`,`stm_202404`,`stm_202405`,`stm_202406`,`stm_202407`,
                              `stm_202408`,`stm_202409`,`stm_202410`,`stm_202411`,`stm_202412`,`stm_202501`,`stm_202502`,
                              `stm_202503`,`stm_202504`,`stm_202505`,.after = Stm_Hoy)


Consol1 <- Consol1 %>%
  mutate(
    Binario_202401 = if_else(stm_202401 == "Activo Normal", 1, 0),
    Binario_202402 = if_else(stm_202402 == "Activo Normal", 1, 0),
    Binario_202403 = if_else(stm_202403 == "Activo Normal", 1, 0),
    Binario_202404 = if_else(stm_202404 == "Activo Normal", 1, 0),
    Binario_202405 = if_else(stm_202405 == "Activo Normal", 1, 0),
    Binario_202406 = if_else(stm_202406 == "Activo Normal", 1, 0),
    Binario_202407 = if_else(stm_202407 == "Activo Normal", 1, 0),
    Binario_202408 = if_else(stm_202408 == "Activo Normal", 1, 0),
    Binario_202409 = if_else(stm_202409 == "Activo Normal", 1, 0),
    Binario_202410 = if_else(stm_202410 == "Activo Normal", 1, 0),
    Binario_202411 = if_else(stm_202411 == "Activo Normal", 1, 0),
    Binario_202412 = if_else(stm_202412 == "Activo Normal", 1, 0),
    Binario_202501 = if_else(stm_202501 == "Activo Normal", 1, 0),
    Binario_202502 = if_else(stm_202502 == "Activo Normal", 1, 0),
    Binario_202503 = if_else(stm_202503 == "Activo Normal", 1, 0),
    Binario_202504 = if_else(stm_202504 == "Activo Normal", 1, 0),
    Binario_202505 = if_else(stm_202505 == "Activo Normal", 1, 0)
  )

write_xlsx(Consol1,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Analisis_Cosechas.xlsx")


### Sección No Cruza ###

No_Cruza <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/CC no cruce.xlsx")

No_Cruza <- No_Cruza %>% inner_join(Stm_Hoy %>% select(ASO_IDENTIFICACION,ESTM_NOMBRE,ASO_FECHA_CAMBIO_ESTADO_MULT), by = c("Identificación Asociado" = "ASO_IDENTIFICACION"))

No_Cruza <- No_Cruza %>% rename("Stm_Hoy" = ESTM_NOMBRE,
          "Fecha Estado" = ASO_FECHA_CAMBIO_ESTADO_MULT)

write_xlsx(No_Cruza,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/CC no cruce.xlsx")



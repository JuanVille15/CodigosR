library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)
library(dbplyr)
library(lubridate)
library(janitor)
library(readr)
library(Microsoft365R)
library(blastula)
library(janitor)

## Conexión SQL ##

Connecion_SQL <- dbConnect( 
    odbc(),
    Driver = "Oracle in instantclient_21_3",
    DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
    SVC = "GRC", 
    encoding = "ISO-8859-1", 
    UID = "JJVT9593", 
    PWD = "KysCoL_140725*")


## Leemos reporte 10 -- ##

db <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo/Reporte10_2025-07-29.xlsx")

## Estatutarios Hoy ##

df1 <- dbGetQuery(Connecion_SQL,glue_sql("SELECT FAC.ASO_IDENTIFICACION,
                                         FAC.CON_CODIGO,
                                         FAC.FACT_VALOR_VENCIDO AS VALOR_VENCIDO,
                                         FAC.FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS VALOR_TOTAL,
                                         ASO_FECHA_CAMBIO_ESTADO_MULT AS FECHA_CAMBIO_STM
                                         FROM GRC.STG_GCC_FACTURACION FAC
                                         LEFT JOIN GRC.GCC_ASOCIADO ASO
                                         ON FAC.ASO_IDENTIFICACION = ASO.ASO_IDENTIFICACION
                                         WHERE ASO.ESTM_CODIGO = 14
                                         AND ASO.ASO_SUBCAMPANNA_ASIGNADA NOT IN ('CSSALTAPRIORID','NOLLAMARASOCI','CSSBAJAPRIORID')
                                         AND FAC.CON_CODIGO IN('APORTES','CALAMID','RECREA','AUX.FUN.','SOLIDAR.','SOLIDARE');"))


db <- db %>% select(`CEDULA ASOCIADO`,`NOMBRE ASOCIADO`,SUBCAMPAÑA, `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,`ANTIGUEDAD DEL ASOCIADO EN COOMEVA`,REGIONAL)

df1_pivot <- df1 %>% select(ASO_IDENTIFICACION,CON_CODIGO,VALOR_VENCIDO, FECHA_CAMBIO_STM) %>% 
                                pivot_wider(
                                    names_from = CON_CODIGO,
                                    values_from = VALOR_VENCIDO,
                                    values_fill = 0
                                )

df1_pivot <- df1_pivot %>% left_join(db %>% select(`CEDULA ASOCIADO`,`NOMBRE ASOCIADO`,SUBCAMPAÑA,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,`ANTIGUEDAD DEL ASOCIADO EN COOMEVA`,REGIONAL), by = c("ASO_IDENTIFICACION" = "CEDULA ASOCIADO"))

df1_pivot <- df1_pivot %>% select(ASO_IDENTIFICACION,`NOMBRE ASOCIADO`,REGIONAL,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,`ANTIGUEDAD DEL ASOCIADO EN COOMEVA`,SUBCAMPAÑA,FECHA_CAMBIO_STM,everything())

df1_pivot <- df1_pivot %>% rename(ESTADO_HOY = `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,
                                  ANTIGUEDAD = `ANTIGUEDAD DEL ASOCIADO EN COOMEVA`)

df1_pivot <- df1_pivot %>% mutate(Vencido_Fondo_mutual = (AUX.FUN. + SOLIDAR. + SOLIDARE))

inact_ACR <- df1_pivot %>% filter(Vencido_Fondo_mutual == 0)

## Cruzamos el perfil del asociado ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/0. Analitica del area/Perfil Estrategico"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Perfil_Asociado <- read_excel(ultimo_archivo)


inact_ACR <- inact_ACR %>% left_join(Perfil_Asociado %>% select(strIdentificacion,strAgrupacionPerfil), by = c("ASO_IDENTIFICACION" = "strIdentificacion"))


conteo_ACR_sub <- inact_ACR %>% group_by(SUBCAMPAÑA) %>% 
                        summarise(Qasociados = n())

Conteo_perfiles <- inact_ACR %>% group_by(strAgrupacionPerfil) %>% 
                            summarise(Qasociados = n())

## Posibles - ACR ##

inact_ACR <- inact_ACR %>% filter(!SUBCAMPAÑA %in%c("CR EMPRESA", "CR INSOLVENCIA", "JURIDICOS CR"))

# Parte 1 ##

SM1 <- inact_ACR %>% filter(strAgrupacionPerfil %in%c("Estratégico", "Táctico"))

SM2 <- inact_ACR %>% filter(strAgrupacionPerfil == "Operativo")

SM2 <- SM2 %>% arrange(desc(ANTIGUEDAD))

SM2 <- SM2[1:34,]

SM_DEF <- SM1 %>% bind_rows(SM2)

CONTEO_DEF <- SM_DEF %>% group_by(SUBCAMPAÑA,strAgrupacionPerfil) %>% 
                                    summarise(QAsociados = n())

write_xlsx(list("SM" = SM_DEF, "Resumen" = CONTEO_DEF),"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/SM_INACTIVOS_MT202507.xlsx")

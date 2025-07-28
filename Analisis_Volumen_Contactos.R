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
library(purrr)
library(stringr)

files <- list.files(path = "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2025/Gestiones", full.names = TRUE)

gestiones <- map_df(files, function(file){
    read_excel(file) %>% 
        mutate(Periodo = substr(basename(file),1,6))
})


may <- read_csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Mayo/202505.csv")

may <- may %>% mutate(Periodo = "202505")

colnames(may) <- colnames(gestiones)

may <- may %>% mutate(identificacion_deudor = as.character(identificacion_deudor),
                      telefono = as.character(telefono),
                      perfil_cliente = as.character(perfil_cliente))
gestiones <- gestiones %>% bind_rows(may)

jun <- read_csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506.csv")

jun <- jun %>% mutate(Periodo = "202506")

colnames(jun) <- colnames(gestiones)

jun <- jun %>% mutate(identificacion_deudor = as.character(identificacion_deudor),
                      telefono = as.character(telefono),
                      perfil_cliente = as.character(perfil_cliente))

gestiones <- gestiones %>% bind_rows(jun)

gestiones <- gestiones %>% arrange(Periodo)

gestiones <- gestiones %>% select(Periodo, everything())

tipos_accion <- gestiones %>% distinct(accion,casa_cobranza)

gestiones <- gestiones %>% filter(!accion %in% c("LLAMADA RETENC TELEF", "GESTION OFICINA","ANILLO DE GEST CCTO", "INACTIVO_RETENCION"))

tipos_accion <- tipos_accion %>% arrange(casa_cobranza)

gestiones <- gestiones %>% mutate(accion = if_else(str_detect(accion, "LLAMADA") & casa_cobranza == "PROCOBAS", "LLAMADA_CREDITOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else( accion %in% c("LLAM INACT CEL INACT", "LLAM RECAUDO REGIONA") & casa_cobranza == "PROCOBAS", "LLAMADA_CREDITOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(str_detect(accion,"WHATSAPP") & casa_cobranza == "PROCOBAS", "WHATSAPP_CREDITOS", accion))

gestiones <- gestiones %>% mutate(accion = if_else(str_detect(accion, "ENVIO COMUNICACION") & casa_cobranza == "PROCOBAS", "SMS_CREDITOS", accion))

gestiones <- gestiones %>% filter(casa_cobranza != "RECAUDOREGIONAL")

gestiones <- gestiones %>% mutate(accion = if_else(accion %in% c("LLAMADA CREDITOS CSS", "LLAMADA CASACOBRANZA"),"LLAMADA_CREDITOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "LLAMADA RECAUDO" & casa_cobranza == "DIR NAC RECAUDO","LLAMADA_DIRNAC",accion))

gestiones <- gestiones %>% filter(!accion %in% c("LLAMADA_DIRNAC", "ENVIO_MENSAJE_DN"))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "ENVIO COMUNICACION" & casa_cobranza == "DIR NAC RECAUDO" & usuario %in% c("SECP1527", "EDRC7754"), "SMS_CREDITOS", accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "ENVIO COMUNICACION" & casa_cobranza == "DIR NAC RECAUDO" & usuario == "JJVT9593", "SMS_ACTIVOS", accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "ENVIO COMUNICACION" & casa_cobranza == "DIR NAC RECAUDO" & usuario == "DSME7993", "SMS_INACTIVOS", accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "ENVIO COMUNICACION" & casa_cobranza == "DIR NAC RECAUDO", "SMS_ACTIVOS", accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion %in% c("LLAMADA RECAUDO CCTO","LLAM RECAUDO REGIONA"),"LLAMADA_ACTIVOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "LLAM INACT CEL INACT","LLAMADA_INACTIVOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion %in% c("WHATSAPP INACTIVOS","WHATSAPP_INACTIVOS"),"WHATSAPP_INACTIVOS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion %in% c("ENVIO COMUNICACIÓN","ENVIO COMUNICACION"),"SMS_CSS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "ENVIO_EMAIL_ACTOR","EMAIL_CSS",accion))

gestiones <- gestiones %>% mutate(accion = if_else(accion == "WHATSAPP","WHATSAPP_ACTIVOS",accion))

resumen <- gestiones %>% group_by(Periodo,accion) %>% 
                            summarise(Qgestiones = n())

promedios <- resumen %>% group_by(accion) %>% 
                                summarise(Promedio_mes = round(mean(Qgestiones),0))

promedios <- promedios %>% arrange(desc(Promedio_mes))

write_xlsx(list("DB" = resumen, "Volumen" = promedios),"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Analisis_Volumetria.xlsx")

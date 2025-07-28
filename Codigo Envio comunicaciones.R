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
library(scales)

##conflicts_prefer(stats::filter)
  
  #Recordar Cambiar el mes y el año para elegir la carpeta correcta del reporte ###########
  ##OJO## Tener en cuenta que el ultimo_archivo si sea el que necesito
  
  ## R-10##
  
  ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"
  
  archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
  
  archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
  
  ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
  
  Reporte_10 <- read_excel(ultimo_archivo)
  
  ### Cargue base Alivio Financiero ################################################################### 
  ##OJO## Tener en cuenta que el ultimo_archivo si sea el que necesito
  
  ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Alivio Financiero/2025"
  
  archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
  
  archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
  
  ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
  
  base_alivio <- read_excel(ultimo_archivo,sheet="DATA SIMULADOR")%>% 
    clean_names(case="all_caps")
  
  ### Cargue anulacion ACR ###################################################################
  ##OJO## Tener en cuenta que el ultimo_archivo si sea el que necesito
  
  ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Anulacion ACR/2025"
  
  archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))
  
  archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
  
  ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
  
  base_anulacion <- read_excel(ultimo_archivo) %>% 
    clean_names(case="all_caps")
  

  ###GESTIONES PROVISIONALES ###################
  
  
data_local_sac_rnp <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Gestiones_provisionales.xlsx") %>% 
    clean_names(case = "all_caps") %>% 
    select(-CONTACTO) %>% 
    transform(IDENTIFICACION_DEUDOR=as.numeric(IDENTIFICACION_DEUDOR))
  
homologacion_contacto <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Homologacion Contactos.xlsx")
  
data_local_sac_rnp <- data_local_sac_rnp %>%
    left_join(homologacion_contacto, by = c("RESPUESTA" = "RES_NOMBRE_RESPUESTA"))
  
data_local_sac_rnp <-data_local_sac_rnp %>% 
    group_by(IDENTIFICACION_DEUDOR) %>% 
    summarize(suma_contact = sum(CONTACTO))
  
data_local_sac_rnp <-data_local_sac_rnp %>%
    mutate(suma_contact,Comunicaciones=ifelse(suma_contact > 0,"No Aplica Masivo","Aplica masivo"))
  
  
Reporte_10 <- Reporte_10 %>% 
    left_join(data_local_sac_rnp %>% select(IDENTIFICACION_DEUDOR,Comunicaciones), by = c("CEDULA ASOCIADO" = "IDENTIFICACION_DEUDOR"))
  
Reporte_10 <- Reporte_10 %>%
    mutate(Comunicaciones=ifelse(is.na(Comunicaciones),"Aplica masivo",Comunicaciones))
  
Reporte <- Reporte_10 %>% select(`CEDULA ASOCIADO`,
                                   `TIPO CLIENTE`,
                                   `NOMBRE ASOCIADO`,
                                   `EDAD CARTERA HOY`,
                                   `ACTOR DE GESTIÓN HOY`,
                                   SUBCAMPAÑA,
                                   `TELÉFONO CELULAR`,
                                   `E-MAIL`,
                                   Comunicaciones)



Reporte <- Reporte %>%
  mutate(`EDAD CARTERA HOY` = as.numeric(`EDAD CARTERA HOY`)) %>%
  filter(Comunicaciones == "Aplica masivo",
         `EDAD CARTERA HOY` > 0,
         `TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))

  
Reporte <- Reporte %>% 
    mutate(`EDAD CARTERA HOY`=ifelse(`EDAD CARTERA HOY`>=4,4,`EDAD CARTERA HOY`))
  
  sub_aplica <- c('BANCOOMEVA_U',
                  'CSS MORA 2',
                  'CSS MORA 3',
                  'CSS MORA 4',
                  'CSS MORA 5',
                  'CSS MORA 6',
                  'CSS MORA 7',
                  'CSSMORA1CORTE10',
                  'CSSMORA1CORTE15',
                  'CSSMORA1CORTE20',
                  'CSSMORA1CORTE25',
                  'CSSMORA1CORTE30',
                  'CSSMORA1CORTE5',
                  'REG BOGOTA',
                  'REG CARIBE',
                  'REG EJECAFETERO',
                  'REG MEDELLIN',
                  'REG PALMIRA',
                  'REG CALI'
  )
  
  Reporte <- Reporte %>% 
    filter(SUBCAMPAÑA %in% sub_aplica)
  
  Reporte <- Reporte %>% 
    mutate(SUBCAMPAÑA=ifelse(SUBCAMPAÑA=='BANCOOMEVA_U',"CSS",SUBCAMPAÑA))
  
  Reporte <- Reporte %>% 
    mutate(SUBCAMPAÑA=ifelse(startsWith(SUBCAMPAÑA, "CSS"),"CSS",SUBCAMPAÑA))
  
  numeros_entrada <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/numeros_entrada.xlsx")
  
  Reporte <- Reporte %>%
    left_join(numeros_entrada, by= c("SUBCAMPAÑA"="ACTOR"))
  
  Reporte <- Reporte %>% clean_names(case = "all_caps")
  
  Reporte <- transform(
    Reporte,TELEFONO_CELULAR = as.numeric(TELEFONO_CELULAR))
  
  Reporte$NOMBRE_PROPIO <-  sapply(strsplit(Reporte$NOMBRE_ASOCIADO, " "), function(x) x[1]) 
  
  Reporte <- Reporte %>% 
    mutate(NOMBRE_ASOCIADO=NOMBRE_PROPIO) %>% 
    select(-NOMBRE_PROPIO)
  
  Reporte$NOMBRE_ASOCIADO <- toTitleCase(tolower(Reporte$NOMBRE_ASOCIADO))
  
  ## BASE COMUNICACIONES SMS
  
  Reporte_sms <- Reporte %>%
    filter(TELEFONO_CELULAR>3000000000,TELEFONO_CELULAR<3510000000,TELEFONO_CELULAR!=is.na(TELEFONO_CELULAR))
  
  ##CRUCE ENCUESTA DE CONTACTABILIDAD
  
  encuentas_contactabilidad <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Archivos de gestión/Ley de intimidad/Autorizaciones_Contactabilidad_Cobranza_Asociados.xlsx") %>% 
    clean_names(case= "all_caps") 
  
  encuentas_contactabilidad <- transform(
    encuentas_contactabilidad,IDENTIFICACION = as.numeric(IDENTIFICACION))
  
  Reporte_sms <- Reporte_sms %>% 
    left_join(encuentas_contactabilidad %>% select(IDENTIFICACION,MENSAJE_DE_TEXTO),by=c("CEDULA_ASOCIADO"="IDENTIFICACION"))
  
  Reporte_sms <- Reporte_sms %>%
    rename(MENSAJE_TEXTO= MENSAJE_DE_TEXTO)
  
  Reporte_sms <- Reporte_sms %>%
    mutate(MENSAJE_TEXTO=ifelse(is.na(MENSAJE_TEXTO),"SI",MENSAJE_TEXTO))
  
  Reporte_sms <- Reporte_sms %>%
    filter(MENSAJE_TEXTO=="SI")
  
  ##### CRUCE BASES PARA OFERTA #############################################################
  
  Reporte_sms <- Reporte_sms %>% 
    left_join(base_alivio %>% select(ASO_IDENTIFICACION,
                                     PORCENTAJE_CONDONACION),by=c("CEDULA_ASOCIADO"="ASO_IDENTIFICACION"))
  
  base_anulacion <- base_anulacion %>% rename(PORCENTAJE_DE_ANULACION = PORCENTAJE_DE_CONDONACION)
  
  Reporte_sms <- Reporte_sms %>% 
    left_join(base_anulacion %>% select(ASO_IDENTIFICACION,
                                        PORCENTAJE_DE_ANULACION),by=c("CEDULA_ASOCIADO"="ASO_IDENTIFICACION"))
  
  #### bases finales para envio ###############################################################
  
  Alivio <- Reporte_sms %>% 
    filter(PORCENTAJE_CONDONACION!=is.na(PORCENTAJE_CONDONACION),
           EDAD_CARTERA_HOY >= 3) %>% 
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR,TELEFONO,PORCENTAJE_CONDONACION) %>% 
    transform(PORCENTAJE_CONDONACION= percent(PORCENTAJE_CONDONACION))
  
  ACR <- Reporte_sms %>%
    anti_join(Alivio,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>% 
    filter(PORCENTAJE_DE_ANULACION!=is.na(PORCENTAJE_DE_ANULACION),
           EDAD_CARTERA_HOY >= 2)%>% 
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR,TELEFONO,PORCENTAJE_DE_ANULACION)%>% 
    transform(PORCENTAJE_DE_ANULACION=round(PORCENTAJE_DE_ANULACION,digits = 2)) %>% 
    transform(PORCENTAJE_DE_ANULACION= percent(PORCENTAJE_DE_ANULACION))
  
  mora1 <- Reporte_sms %>% 
    filter(EDAD_CARTERA_HOY==1) %>%
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR)
  
  mora2 <- Reporte_sms %>% 
    anti_join(ACR,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>%
    filter(EDAD_CARTERA_HOY==2) %>%
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR)
  
  mora3 <- Reporte_sms %>%
    anti_join(ACR,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>%
    anti_join(Alivio,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>%
    filter(EDAD_CARTERA_HOY==3) %>%
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR,TELEFONO)
  
  mora4 <- Reporte_sms %>%
    anti_join(ACR,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>%
    anti_join(Alivio,by=("CEDULA_ASOCIADO"="CEDULA_ASOCIADO")) %>%
    filter(EDAD_CARTERA_HOY==4) %>%
    select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,TELEFONO_CELULAR,TELEFONO)
  
  ### Generacion de bases #########################################################
  
  v_today <- Sys.Date()
  
  write_xlsx(Alivio,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/Alivio_{v_today}.xlsx"))
  
  write_xlsx(ACR,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/ACR_{v_today}.xlsx"))
  
  write_xlsx(mora1,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/Mora1_{v_today}.xlsx"))
  
  write_xlsx(mora2,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/Mora2_{v_today}.xlsx"))
  
  write_xlsx(mora3,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/Mora3_{v_today}.xlsx"))
  
  write_xlsx(mora4,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado/Mora4_{v_today}.xlsx"))

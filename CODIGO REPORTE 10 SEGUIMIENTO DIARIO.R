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

##conflicts_prefer(dplyr::filter) XD

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

### ---- CORRECCIÓN ASIGNACIÓN MORA 0 CR ------ ###


## Actualizar Campaña MORA 0 CR ##

cr_mora0_hoy <- data %>% filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"),
                            SUBCAMPAÑA == "MORA 0 CR",
                            `TIPO CARTERA` == "M",
                            `EDAD CARTERA HOY` > 0)

## Validacion ## 

cr_mora0 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/CR MORA 0/Resultado.xlsx")


cr_mora0_hoy <- cr_mora0_hoy %>% filter(! `CEDULA ASOCIADO` %in% cr_mora0$`CEDULA ASOCIADO`) ## Aca se dejan solo los registros que no aparecen en la inicial ##


cr_mora0 <- cr_mora0 %>% bind_rows(cr_mora0_hoy) ## Se pegan debajo los registros nuevos

write_xlsx(cr_mora0, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/CR MORA 0/Resultado.xlsx") ## Se sobre escribe la nueva foto ##

cr_mora0 <- cr_mora0 %>% mutate(`EDAD CARTERA INICIAL` = if_else(is.na(`EDAD CARTERA INICIAL`),0,`EDAD CARTERA INICIAL`))

cr_mora0 <- cr_mora0 %>% mutate(`EDAD CARTERA HOY` = if_else(is.na(`EDAD CARTERA HOY`),0,`EDAD CARTERA HOY`))

cr_mora0 <- cr_mora0 %>% mutate(`ACTOR DE GESTIÓN HOY` = "CSS",
                                  SUBCAMPAÑA = if_else(`EDAD CARTERA HOY` == 1,"CSSMORA1CORTE5","CSS MORA 2"),
                                  `USUARIO ASIGNADO` = if_else(SUBCAMPAÑA == "CSSMORA1CORTE5", "ALEJANDRO MOMPOTES","DANNY YULIETH VILLA PIRAQUIVE"))

## Se filtran las campañas del r10 que son ##

data <- data %>%
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))


data <- data %>%
  filter(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo","Receso"))

Subcampaña <- c("CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                "CSSMORA1CORTE30", "CSSMORA1CORTE5", "REG BOGOTA", "REG CALI", "REG CARIBE", "REG EJECAFETERO",
                "REG MEDELLIN", "REG PALMIRA","CSSCUOTA0HASTA6","CARTERA CASTIGO","CSSALTAPRIORID","CSSBAJAPRIORID","BANCOOMEVA_U","MORA >= 4 CR",
                "MORA 1 CR","MORA 2 - 3 CR")


data <- data %>%
  filter(`SUBCAMPAÑA` %in% Subcampaña )

data <- data %>% bind_rows(cr_mora0)

### ---- FIN ---- ##


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

# Extraer el día del corte#

R_10 <- R_10 %>%
  mutate(DIA_CORTE = as.numeric(sub(".*Al (\\d{2}) del Mes.*", "\\1", `DESCRIPCIÓN DEL CORTE`)))

## Matriz Corte ##

matriz_cortes <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Cortes.xlsx")

# Hacer un join para agregar la columna de caída correspondiente al día de corte #
R_10 <- R_10 %>%
  left_join(matriz_cortes, by = c("DIA_CORTE" = "Dia_Corte"))

# Obtener el día actual #
dia_actual <- as.numeric(format(Sys.Date(), "%d"))

# Definir si el corte ya pasó #
R_10 <- R_10 %>%
  mutate(CORTE_PASADO = ifelse(dia_actual >= Caida, TRUE, FALSE))

## Crear la columna ESTADO CARTERA ##

R_10 <- R_10 %>%
  mutate(ESTADO_CARTERA = case_when(
    CORTE_PASADO & `EDAD CARTERA HOY` == 0 ~ "NORMALIZA",
    CORTE_PASADO & `EDAD CARTERA HOY` < `EDAD CARTERA INICIAL` ~ "MEJORA",
    CORTE_PASADO & `EDAD CARTERA HOY` == `EDAD CARTERA INICIAL` ~ "MANTIENE",
    CORTE_PASADO & `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 0 & `EDAD CARTERA HOY` == 0 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 1 & `EDAD CARTERA HOY` == 1 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 1 & `EDAD CARTERA HOY` == 0 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 2 & `EDAD CARTERA HOY` == 2 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 2 & `EDAD CARTERA HOY` == 1 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 2 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 3 & `EDAD CARTERA HOY` == 3 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 3 & `EDAD CARTERA HOY` == 2 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 3 & `EDAD CARTERA HOY` == 1 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 3 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 4 & `EDAD CARTERA HOY` == 4 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 4 & `EDAD CARTERA HOY` == 3 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 4 & `EDAD CARTERA HOY` == 2 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 4 & `EDAD CARTERA HOY` == 1 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 4 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 5 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 4 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 3 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 2 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 1 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 5 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 6 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 5 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 4 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 3 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 2 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 1 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 6 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 7 ~ "POR VENCER",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 6 ~ "MANTIENE",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 5 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 4 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 3 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 2 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 1 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA INICIAL` == 7 & `EDAD CARTERA HOY` == 0 ~ "MEJORA",
    !CORTE_PASADO & `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
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
Base_gestiones <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Temporal/Gestiones Provisionales/Gestiones_Mensuales.xlsx", col_types = "text")

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

## Encontrando la ultima fehca de gestión x asociado ##

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

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Alivios Economicos/Anulacion ACR/2025"
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
  mutate(Alivio_financiero = "NO")

R_10 <- R_10 %>%
  distinct()

## Columna Gestion Reciente ##

R_10 <- R_10 %>% mutate(fecha_ultima_gestion = as.Date(fecha_ultima_gestion, format ="%d/%m/%Y" ))

R_10 <- R_10 %>%
  mutate(Gestion_Reciente = if_else(
    is.na(fecha_ultima_gestion) | 
      ((Sys.Date()-1) - fecha_ultima_gestion) > 3, 
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

R_10 <- R_10 %>%
  relocate(Caida, .after = last_col())

## Filtrar solo CSS ##

Campañas_css <- c("CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                  "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                  "CSSMORA1CORTE30", "CSSMORA1CORTE5", "REG BOGOTA", "REG CALI", "REG CARIBE", "REG EJECAFETERO",
                  "REG MEDELLIN", "REG PALMIRA","CSSCUOTA0HASTA6")

R_10_css <- R_10 %>% filter(`SUBCAMPAÑA` %in% Campañas_css )

## Creamos la columna antiguedad en meses ## 

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KysCoL_140725*")

## Consulta Fecha Ingreso Asociado ##

Fech_Ingreso <- dbGetQuery(Connecion_SQL,glue_sql("select aso_identificacion,
                                                          aso_fecha_ingreso
                                                      from grc.gcc_asociado;"))

Fech_Ingreso <- Fech_Ingreso %>% mutate(ASO_IDENTIFICACION = as.double(ASO_IDENTIFICACION))

Fech_Ingreso <- Fech_Ingreso %>% group_by(ASO_IDENTIFICACION) %>% 
                                    slice_max(order_by = ASO_FECHA_INGRESO, with_ties = FALSE) %>% 
                                                ungroup()

R_10_css <- R_10_css %>% left_join(Fech_Ingreso, by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))

hoy <- Sys.Date()

R_10_css <- R_10_css %>% mutate(ASO_FECHA_INGRESO = as.Date(ASO_FECHA_INGRESO))

R_10_css <- R_10_css %>% mutate(Antiguedad_Meses = (hoy - ASO_FECHA_INGRESO))

R_10_css <- R_10_css %>% mutate(Antiguedad_Meses = Antiguedad_Meses/30) 

R_10_css <- R_10_css %>% mutate(Antiguedad_Meses = floor(as.numeric(Antiguedad_Meses)))

R_10_css <- R_10_css %>% mutate(Rango_Antiguedad = case_when( Antiguedad_Meses > 0 & Antiguedad_Meses <= 6 ~ "0 - 6 meses",
                                                      Antiguedad_Meses > 6 & Antiguedad_Meses <= 12 ~ "6 meses - 1 año",
                                                      Antiguedad_Meses > 12 & Antiguedad_Meses <= 36 ~ "1 año - 3 años",
                                                      Antiguedad_Meses > 36 & Antiguedad_Meses <= 60 ~ "3 años - 5 años",
                                                      TRUE ~ "Mas de 5 Años"))

R_10_css <- R_10_css %>% select(-ASO_FECHA_INGRESO,Antiguedad_Meses)

## Cruzamos el perfil del asociado ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/0. Analitica del area/Perfil Estrategico"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
Perfil_Asociado <- read_excel(ultimo_archivo)

Perfil_Asociado <- Perfil_Asociado %>% mutate(strIdentificacion = as.double(strIdentificacion))

R_10_css <- R_10_css %>% left_join(Perfil_Asociado %>% select(strIdentificacion,strAgrupacionPerfil), by = c("CEDULA ASOCIADO" = "strIdentificacion"))

## Escribimos el R_10 del dia en la Carpeta del CSS ##

ruta_archivo <- paste0("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/7. Julio/R10/R10-", Fecha_hoy,".xlsx")
write_xlsx(R_10_css, ruta_archivo)

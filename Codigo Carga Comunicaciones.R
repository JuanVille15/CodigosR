### Carga Masiva de comunicaciones#################################################

### Librerias #####################################################################
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

### Importante cambiar la fecha de la carpeta para la carga de comunicaciones ######

directorio <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/2025/6. Junio"

# Obtener la lista de archivos en el directorio ##################################
archivos <- list.files(directorio, full.names = TRUE)

# Función para cargar un archivo y agregar el nombre del archivo como una columna
cargar_con_nombre <- function(archivo) {
  # Cargar el archivo
  df <- read_excel(archivo)
  df <- select(df,1:3)
  
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
df_completo <- do.call(rbind, dataframes)

df_completo <- df_completo %>% 
  separate(NOMBRE_ARCHIVO, into = c("TIPO", "FECHA"), sep = "_")
df_completo$FECHA <- gsub("\\.xlsx","", df_completo$FECHA)

df_completo <- df_completo %>%
  transform(FECHA=as.Date(FECHA)) %>% 
  mutate(FECHA=format(FECHA, format = "%d-%m-%Y")) %>% 
  transform(FECHA=as.character(FECHA))

### cargue base Mensajes ########################################################

mensajes <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Base Mensajes.xlsx") %>% 
  clean_names(case ="all_caps")

df_completo <- df_completo %>%
  left_join(mensajes, by=c("TIPO"="TIPO_COMUNICACION"))

df_completo <- df_completo %>%
  mutate(TipoGestion="1",
         CasaDeCobro="DIR NAC RECAUDO",
         Usuario="JJVT9593",
         Gestion="ENVIO COMUNICACION",
         Respuesta="ENVIO_MENSAJE_DN",
         Contacto= "ASOCIADO",
         MotivoDeNoPago= NA,
         FechaProxGestion = NA,
         TiempoGestion = NA,
         NumeroCuenta= NA,
         ValorPromesa = NA,
         fechaPromesa = NA,
         DireccionVisita = NA,
         SubCampaña = NA,
         Canal = NA) %>% 
  select(-TIPO,
         -NOMBRE_ASOCIADO) %>% 
  rename(Identificacion=CEDULA_ASOCIADO,
         FechaGestion=FECHA,
         Observacion=MENSAJE,
         Telefono=TELEFONO_CELULAR) %>% 
  select(TipoGestion,
         Identificacion,
         CasaDeCobro,
         Usuario,
         FechaGestion,
         Gestion,
         Respuesta,
         Contacto,
         Observacion,
         Telefono,
         MotivoDeNoPago,
         FechaProxGestion,
         TiempoGestion,
         NumeroCuenta,
         ValorPromesa,
         fechaPromesa,
         DireccionVisita,
         SubCampaña,
         Canal)

v_today <- Sys.Date()

write_xlsx(df_completo,glue("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargue Comunicaciones SAC/Formato_Carga_Gestiones_Masivas_{v_today}.xlsx"))

str(df_completo)

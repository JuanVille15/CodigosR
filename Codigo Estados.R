library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KyCo_090625*")

Base_Cruce <- dbGetQuery(Connecion_SQL,glue_sql(
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


dbDisconnect(Connecion_SQL)

Cedulas_Consulta <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Estados/Cedulas_Consulta.xlsx")

Base_Cruce <- Base_Cruce %>% mutate(ASO_IDENTIFICACION = as.numeric(ASO_IDENTIFICACION))

Cedulas_Consulta <- Cedulas_Consulta %>% mutate(ASO_IDENTIFICACION = as.numeric(ASO_IDENTIFICACION))

Base_Estados_Dia <- Base_Cruce %>% 
  semi_join(Cedulas_Consulta, by=c("ASO_IDENTIFICACION"="ASO_IDENTIFICACION"))

# Write_xlsx(NOMBRE DEL DF RESULTADO, "RUTA DONDE DESEO GUARDAR EL ARCHIVO *LOS SLASH NORMAL*/NOMBRE DEL DOCUMENTO.XLSX",COL_NAMES=TRUE 
# *COL_NAMES= TRUE*: SIRVE PARA DEFINIR QUE LA PRIMERA FILA SON LOS TITULOS*)

write_xlsx(Base_Estados_Dia,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Estados/Resultado/Base_Estados_Dia.xlsx",col_names=TRUE)

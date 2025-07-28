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


## LEEMOS EL REPORTE 10 X EMPRESAS ##

r10 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/R10XEmpresas/Mayo 2025.xlsx")


## NOS TRAEMOS LA FACTURA DE BANCOOMEVA ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "TyhU_RtQ3491*")



Fac_Banco <- dbGetQuery(Connecion_SQL,glue_sql("SELECT aso_identificacion AS CEDULA_ASOCIADO,
                                               MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) AS CUOTAS_ATRASADAS,
                                               SUM(fact_valor_cuota_mes) AS VALOR_CUOTA_MES,
                                               SUM(fact_valor_vencido) AS VALOR_VENCIDO,
                                               SUM(fact_valor_cuota_mes + fact_valor_vencido) AS FACTURA_TOTAL
                                               FROM grc.stg_gcc_facturacion FAC
                                               LEFT JOIN grc.gcc_concepto CON
                                               ON CON.con_codigo = FAC.con_codigo
                                               LEFT JOIN grc.gcc_agrupacion AGR
                                               ON AGR.agr_codigo = CON.agr_codigo
                                               WHERE AGR.AGE_CODIGO = 1
                                               GROUP BY FAC.aso_identificacion;"))


r10 <- r10 %>% filter(ACTOR_GESTION_HOMOLOGADO == "Bancoomeva")

r10 <- r10 %>% filter(Saldo_menor == "NO" | is.na(Saldo_menor))

r10 <- r10 %>% filter(ESTADO_CARTERA_COOMEVA %in% c ("DETERIORA","POR VENCER"))

r10 <- r10 %>% select(`CEDULA ASOCIADO`,ESTADO_CARTERA_COOMEVA,VALOR_VENCIDO_COOMEVA_HOY,Edad_Inicial_Coomeva,EDAD_HOY_COOMEVA,gestion_base,fecha_ultima_gestion,Qgestiones,ACTOR_GESTION_HOMOLOGADO,ESTADO_CARTERA_MP,VALOR_VENCIDO_MP_HOY)

r10 <- r10 %>% left_join(Fac_Banco %>% select(CEDULA_ASOCIADO, CUOTAS_ATRASADAS,VALOR_VENCIDO), by = c("CEDULA ASOCIADO" = "CEDULA_ASOCIADO"))

r10 <- r10 %>% rename(CUOTAS_ATRASADAS_BANCO = CUOTAS_ATRASADAS,
                       VALOR_VENCIDO_BANCO = VALOR_VENCIDO)

## Consultamos Estado en banco ##

Estados_Banco <- dbGetQuery(Connecion_SQL,glue_sql("SELECT aso_identificacion,
                                                           desc_estado_financiera AS Estado_Banco
                                                           FROM grc.gcc_asociado;"))

r10 <- r10 %>% left_join(Estados_Banco, by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))


write_xlsx(r10, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Resultado/Bases/Bancoomeva/Seg_Banco_Mayo.xlsx")

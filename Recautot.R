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

## Conexión SQL - Developer ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KysCoL_140725*")


## Archivo base ##

Recautot <- dbGetQuery(
  Connecion_SQL,
  glue_sql(   "WITH rbanco AS (
                  SELECT con_codigo,
                         SUM(hrcd_valor_recaudo) AS RECBNCO
                  FROM grc.gcc_historia_recaudo
                  WHERE hrcd_fecha_recaudo >= TO_DATE('01/07/2025', 'DD/MM/YYYY')
                    AND hrcd_tipo_recaudo = 'FIN'
                    AND hrcd_nro_transaccion NOT IN ('71','52','51')
                  GROUP BY con_codigo
                ),
                rmult AS (
                  SELECT con_codigo,
                         SUM(hrcd_valor_recaudo) AS RECMUL
                  FROM grc.gcc_historia_recaudo
                  WHERE hrcd_fecha_recaudo >= TO_DATE('01/07/2025', 'DD/MM/YYYY')
                    AND hrcd_tipo_recaudo = 'MUL'
                    AND hrcd_nro_transaccion NOT IN ('71','52','51')
                  GROUP BY con_codigo
                )
                SELECT con.con_codigo,
                       agr.agr_descripcion,
                       uso.uso_descripcion,
                       RECBNCO,
                       RECMUL
                FROM grc.gcc_concepto con
                LEFT JOIN (
                  SELECT agr_codigo,
                         age_codigo,
                         agr_descripcion
                  FROM grc.gcc_agrupacion
                ) agr ON agr.agr_codigo = con.agr_codigo
                LEFT JOIN (
                  SELECT con_codigo, 
                         uso_codigo
                  FROM grc.gcc_uso_concepto
                ) usoc ON con.con_codigo = usoc.con_codigo
                LEFT JOIN (
                  SELECT uso_codigo,
                         uso_descripcion
                  FROM grc.gcc_uso
                ) uso ON usoc.uso_codigo = uso.uso_codigo
                LEFT JOIN rbanco ON rbanco.con_codigo = con.con_codigo
                LEFT JOIN rmult  ON rmult.con_codigo = con.con_codigo
                WHERE RECBNCO > 0
                   OR RECMUL > 0;")
            )


## Mod Base ###

Recautot <- Recautot %>% distinct(CON_CODIGO, .keep_all = TRUE)

Recautot <- Recautot %>% mutate(RECBNCO = if_else(is.na(RECBNCO),0,RECBNCO),
                                RECMUL = if_else(is.na(RECMUL),0,RECMUL))

Recautot <- Recautot %>% mutate(WCODPER = 202507, ## Periodo Analisis ## Cambiar según se necesite
                                RECTOTL = RECBNCO + RECMUL,
                                ULFECREC = "",
                                OTRBNCO = 0,
                                OTRMULT = 0,
                                OTRTOTL = 0,
                                TOTBNCO = RECBNCO,
                                TOTMULT = RECMUL,
                                TOTRECA = RECBNCO + RECMUL,
                                FECPROA = format(Sys.Date(), "%Y%m%d"),
                                HORPROA = format(Sys.time(), "%H%M%S"))
                                


Recautot <- Recautot %>% rename(WCODCPT = CON_CODIGO,
                                WCODCON = AGR_DESCRIPCION,
                                RECBNCO = RECBNCO,
                                RECMULT = RECMUL)

Recautot <- Recautot %>% mutate(WCODCON = case_when(WCODCON == "Aportes de los Asociados" ~ "APORTES",
                                                    WCODCON == "Auxilios Funerarios" ~ "AUXIFUN",
                                                    WCODCON == "Calamidad" ~ "CALAMID",
                                                    WCODCON == "Creditos Coomeva" ~ "CRESOLID",
                                                    WCODCON == "Seguro de Vida Coomeva" ~ "SG.VIDAM",
                                                    WCODCPT == "ECMPFTF" ~ "PAGOSEG",
                                                    WCODCON == "Seguros Coomeva" ~ "PAGO SEG",
                                                    WCODCON == "Medicina Prepagada" ~ "01TRAD",
                                                    WCODCON == "Recreacion" ~ "RECREA",
                                                    WCODCON == "Solidaridad" ~ "SOLIDARI",
                                                    WCODCPT == "PAGOINTM" ~ "PAGOINTM",
                                                    TRUE ~ ""))
Recautot <- Recautot %>% filter(WCODCON != "")

Recautot <- Recautot %>% rename(ESTATUT = USO_DESCRIPCION)

## Consultamos los conceptos estatutarios ##

Estatutarios <- dbGetQuery(Connecion_SQL, glue_sql("SELECT
                                                    con.con_codigo,
                                                    uso.uso_descripcion
                                                  from grc.gcc_concepto con
                                                      left join (select con_codigo, 
                                                                 uso_codigo
                                                                 from grc.gcc_uso_concepto) usoc
                                                  on con.con_codigo =  usoc.con_codigo
                                                      left join (select uso_codigo,
                                                                 uso_descripcion
                                                                from grc.gcc_uso) uso
                                                                on usoc.uso_codigo = uso.uso_codigo
                                                   where uso.uso_descripcion = 'Conceptos Estatutarios';"))



Recautot <- Recautot %>% mutate(ESTATUT = if_else(WCODCPT %in% Estatutarios$CON_CODIGO,"S","N"))

Recautot <- Recautot %>% select(WCODPER,
                                WCODCPT,
                                WCODCON,
                                RECBNCO,
                                RECMULT,
                                RECTOTL,
                                ULFECREC,
                                ESTATUT,
                                OTRBNCO,
                                OTRMULT,
                                OTRTOTL,
                                TOTBNCO,
                                TOTMULT,
                                TOTRECA,
                                FECPROA,
                                HORPROA)


Recautot <- Recautot %>% mutate(WCODCON = if_else(WCODCPT == "PRIMANIV","SOLIDARI",WCODCON))

dbDisconnect(Connecion_SQL)

write_xlsx(Recautot,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/GCC/Recautot/Resultado.xlsx")

##---- Proceso Tesoreria ##

Recautot <- Recautot %>% mutate(WCODCON = case_when(WCODCPT %in% c("CR.LI.M","CRESOLID","CREDIASO","REESTCRS") ~ "CRESOLID",
                                                    WCODCPT %in% c("CRPATRIM","CRCUOINI") ~ "CREDPATRIM",
                                                    WCODCPT %in% c("CR.LBRM", "CR.EDUCM") ~ "CARTERA BANCO",
                                                    TRUE ~ WCODCON))

write_xlsx(Recautot,"//coomeva.nal/dfscoomeva/Gesfin/08. SOPORTE/INFORME RECAUDOS/BD CARTERA.xlsx")

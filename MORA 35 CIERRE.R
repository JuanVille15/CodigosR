#NUEVA BASE CAMPAÑA MORA 35

#CARGA DE LIBRERIAS


library(dplyr)
library(readxl)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(lubridate)
library(stringr)
library(tidyr)


#--------------------------------------------- CONEXIÓN A GCC -------------------------------------------------------------------

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_13",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "Jv43#j9s39")

# Connecion_SQL <- dbConnect( 
#   odbc(),
#   Driver = "Oracle in instantclient_21_3",
#   DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
#   SVC = "GRC", 
#   encoding = "ISO-8859-1", 
#   UID = "GRC", 
#   PWD = "GrCooM_23#")



#--------------------------------------------- CONSULTA POLIZAS DIARIA ---------------------------------------------------

Mora35 <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT 
    fac.*,
    cor.cor_descripcion,
    cli.NOMBRE_COMPLETO,
    regi.reg_nombre,
    regi.estm_nombre,
    cor.cor_dia_corte,
    agr.AGR_DESCRIPCION,
    axe.age_nombre,
    sac.estado_credito,
    gest.GES_FECHA_GESTION,
    gest.AUX_USUARIO,
    gest.CAS_NOMBRE_CASA,
    gest.ACC_NOMBRE_ACCION,
    gest.RES_NOMBRE_RESPUESTA,
    gest.ACT_ACTOR_GESTION,
    gest.HGES_VALOR_PROMESA,
    gest.HGES_ESTADO_PROMESA,
    tel.tel_numero_telefono,
    gol.celular as TEL_NUMERO_GOLDEN,
    gol.email_1,
    gol.direccion_residencia,
    cli.tel1,
    cli.tel2,
    cli.tel3,
    cli.cel,
    cli.exte
  FROM (SELECT
        GCC_TIPO_ALFABETICO,
        ASO_IDENTIFICACION,
        CON_CODIGO,
        cor_numero_corte,
        FACT_DIAS_DE_MORA,
        FACT_INTERES_MORA_DIA,
        FACT_VALOR_CUOTA_MES,
        FACT_VALOR_VENCIDO,
        FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS TOTAL,
        FACT_NUMERO_CUOTAS_ATRASADAS,
        FACT_NUMERO_PAGARE
        FROM GRC.stg_gcc_facturacion
        WHERE  CON_CODIGO IN ('AUT PESA',
                        'AUTLIB',
                        'AUTLIV',
                        'FLE.HOG+',
                        'HOGMASBA',
                        'RC MED',
                        'SEG HOGA',
                        'SEG MOTO',
                        'SEG.VEH.',
                        'SEGAUTOS',
                        'SEGVEHBA',
                        'SEGVEHCB')
            AND FACT_DIAS_DE_MORA >=8
            AND FACT_DIAS_DE_MORA < 60) fac
  LEFT JOIN GRC.gcc_concepto c
   ON fac.con_codigo = c.con_codigo
  LEFT JOIN (SELECT 
                AGR_CODIGO,
                AGE_CODIGO,
                AGR_DESCRIPCION
            FROM GRC.gcc_agrupacion
            WHERE AGR_DESCRIPCION NOT IN ('Comodin Coomeva')) agr
   ON c.agr_codigo = agr.agr_codigo
  LEFT JOIN GRC.gcc_agrupacion_empresa axe
   ON agr.age_codigo = axe.age_codigo
  LEFT JOIN (SELECT 
                pagare,
                estado_credito
            FROM GRC.stg_gcc_sac_infoobl 
            WHERE estado_credito not in ('CASTIGADO'))sac
   ON fac.FACT_NUMERO_PAGARE = sac.pagare
  LEFT JOIN (SELECT
            ASO_IDENTIFICACION,
            tel_numero_telefono
            FROM GRC.gcc_telefono 
            WHERE tel_tipo_telefono = 'CEL') tel
   ON fac.ASO_IDENTIFICACION = tel.ASO_IDENTIFICACION
  LEFT  JOIN GRC.stg_gcc_golden_ubic_dat gol
   ON fac.ASO_IDENTIFICACION = gol.numero_identificacion
  LEFT JOIN GRC.gcc_corte_facturacion cor
   ON fac.cor_numero_corte = cor.cor_numero_corte
  LEFT JOIN GRC.stg_gcc_sac_infocli cli
    ON fac.ASO_IDENTIFICACION = cli.aso_identificacion
  LEFT JOIN (SELECT aso_identificacion, reg.reg_nombre, esta.estm_nombre
            FROM GRC.gcc_asociado aso 
            LEFT JOIN GRC.gcc_regional reg
                ON aso.reg_codigo = reg.reg_codigo
            LEFT JOIN GRC.gcc_estados_multiactiva esta
                ON aso.estm_codigo = esta.estm_codigo) regi 
    ON fac.aso_identificacion = regi.aso_identificacion
  LEFT JOIN (SELECT *
            FROM (SELECT 
                    g.ASO_IDENTIFICACION,
                    g.GES_FECHA_GESTION,
                    g.AUX_USUARIO,
                    g.CAS_NOMBRE_CASA,
                    g.ACC_NOMBRE_ACCION,
                    g.RES_NOMBRE_RESPUESTA,
                    g.ACT_ACTOR_GESTION,
                    g.HGES_VALOR_PROMESA,
                    g.HGES_ESTADO_PROMESA,
                    ROW_NUMBER () OVER (PARTITION BY g.aso_identificacion ORDER BY g.GES_FECHA_GESTION DESC) AS rn
                    FROM GRC.GCC_HISTORIA_GESTIONES g
                    WHERE g.GES_FECHA_GESTION > SYSDATE - INTERVAL '2' MONTH
                ) subquery
            WHERE subquery.rn = 1) gest
        ON fac.aso_identificacion = gest.aso_identificacion"))


base <- Mora35 %>%
  rename(TIPO_DOCUMENTO = GCC_TIPO_ALFABETICO,
         CEDULA = ASO_IDENTIFICACION,
         CORTE = COR_DESCRIPCION,
         DIA_CORTE = COR_DIA_CORTE,
         REGIONAL = REG_NOMBRE,
         ESTADO = ESTM_NOMBRE,
         DIAS_DE_MORA = FACT_DIAS_DE_MORA,
         INTERES_MORA_DIA = FACT_INTERES_MORA_DIA,
         VALOR_CUOTA_MES = FACT_VALOR_CUOTA_MES,
         VALOR_VENCIDO = FACT_VALOR_VENCIDO,
         NUMERO_CUOTAS_ATRASADAS = FACT_NUMERO_CUOTAS_ATRASADAS,
         PAGARE = FACT_NUMERO_PAGARE,
         FECHA_GESTION = GES_FECHA_GESTION,
         CASA = CAS_NOMBRE_CASA,
         ACCION = ACC_NOMBRE_ACCION,
         RESPUESTA = RES_NOMBRE_RESPUESTA,
         GESTION = ACT_ACTOR_GESTION,
         VALOR_PROMESA = HGES_VALOR_PROMESA,
         PROMESA = HGES_ESTADO_PROMESA,
         TELEFONO = TEL_NUMERO_TELEFONO,
         NUMERO_GOLDEN = TEL_NUMERO_GOLDEN,
         EMAIL = EMAIL_1,
         NOMBRE = NOMBRE_COMPLETO,
         DIRECCION = DIRECCION_RESIDENCIA) %>%
  select(TIPO_DOCUMENTO,
         CEDULA,
         NOMBRE,
         CORTE,
         DIA_CORTE,
         REGIONAL,
         ESTADO,
         CON_CODIGO,
         DIAS_DE_MORA,
         INTERES_MORA_DIA,
         VALOR_CUOTA_MES,
         VALOR_VENCIDO,
         TOTAL,
         NUMERO_CUOTAS_ATRASADAS,
         PAGARE,
         AGR_DESCRIPCION,
         AGE_NOMBRE,
         ESTADO_CREDITO,
         FECHA_GESTION,
         AUX_USUARIO,
         CASA,
         ACCION,
         RESPUESTA,
         GESTION,
         VALOR_PROMESA,
         PROMESA,
         TELEFONO,
         NUMERO_GOLDEN,
         EMAIL,
         DIRECCION,
         TEL1,
         TEL2,
         TEL3,
         CEL,
         EXTE)


#DEFINICION FECHA
v_today <- Sys.Date()

#CREACION ARCHIVO BASE
base %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/2024/12. Diciembre/Reporte_Mora35_{v_today}_USO INTERNO.xlsx",
      col_names=TRUE,sheet="Hoja1"))

#--------------------------------------------- ARCHIVO GESTIONES ------------------------------------------------------------

ruta_archivo <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/Gestiones/Gestiones_202412.xlsx"
nombre_hoja <- "Hoja1"
gestiones_periodo  <- read_excel(ruta_archivo, sheet = nombre_hoja) %>% 
  clean_names(case="all_caps") %>% 
  filter(USUARIO == "ILMB9539" | USUARIO == "JMCR1010") %>% 
  rename(CEDULA = IDENTIFICACION_DEUDOR) %>% 
  mutate(CEDULA = as.numeric(CEDULA))



#--------------------------------------------- ARCHIVO WOLKVOX ------------------------------------------------------------

BASE <- Mora35 %>% 
  rename(CEDULA = ASO_IDENTIFICACION, TELEFONO = TEL_NUMERO_TELEFONO, NOMBRE = NOMBRE_COMPLETO) %>% 
  left_join(gestiones_periodo %>%  select(CEDULA, FECHA_GESTION, RESPUESTA), by = c("CEDULA" = "CEDULA")) %>% 
  filter(FACT_DIAS_DE_MORA > 8, is.na(RESPUESTA)) %>% 
  distinct(CEDULA, NOMBRE, CON_CODIGO, FACT_NUMERO_PAGARE, TELEFONO) %>% 
  mutate(TELEFONO = as.numeric(TELEFONO))

wolkvox <- data.frame(matrix("", nrow = nrow(BASE),ncol = 35))
wolkvox[,1] <- BASE$NOMBRE
wolkvox[,4] <- BASE$CEDULA
wolkvox[,24] <- paste0(9,BASE$TELEFONO)
wolkvox[,35] <- rep(c(12958, 12912), length.out = nrow(BASE))


write.table(wolkvox, file = "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/Plantilla wolkvox mod mora35.csv",
            row.names = FALSE, col.names = FALSE, sep = ",")




# ****************** EL SEGUIMIENTO VA DESDE AQUI ********************

#--------------------------------------------- ARCHIVO CONSOLIDADO ---------------------------------------------------

lista_archivos <- list.files(path = "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/2024/12. Diciembre",
                             pattern = "*.xlsx", full.names = TRUE) # Listar archivos .xlsx en la carpeta especificada

columnas <- lapply(lista_archivos, function(file) { read_excel(file, n_max = 1, col_types = "text") %>% colnames()}) # Leer la estructura de los archivos y asegurarnos de que tengan las mismas columnas

if (length(unique(sapply(columnas, paste, collapse = ","))) > 1) { stop("Los archivos tienen estructuras de columnas diferentes.")} # Verificar si todas las estructuras de columnas son iguales

Consolidado_Polizas <- lapply(lista_archivos, read_excel, col_types = "text") %>% # Leer todos los archivos y combinarlos en un solo dataframe, convirtiendo todas las columnas a 'character'
  bind_rows()


datos_polizas <- Consolidado_Polizas %>% 
  distinct() %>% 
  mutate(CEDULA = as.numeric(CEDULA)) %>% 
  select(TIPO_DOCUMENTO, CEDULA, NOMBRE, CON_CODIGO, PAGARE, REGIONAL, CORTE, DIA_CORTE, ESTADO, DIAS_DE_MORA, NUMERO_CUOTAS_ATRASADAS)


Consolidado_Polizas <- Consolidado_Polizas %>% 
  arrange(CEDULA, CON_CODIGO, PAGARE, desc(DIAS_DE_MORA)) %>% 
  distinct(CEDULA, CON_CODIGO, PAGARE, .keep_all = TRUE) %>% 
  mutate(CEDULA = as.numeric(CEDULA),
         PAGARE = as.numeric(PAGARE)) %>% 
  select(CEDULA, CON_CODIGO, PAGARE, DIAS_DE_MORA ) 

cedulas_polizas <- Consolidado_Polizas %>% 
  distinct(CEDULA)

id_chunks <- split(cedulas_polizas$CEDULA, ceiling(seq_along(cedulas_polizas$CEDULA) / 1000)) # Divide en bloques de 1000


#--------------------------------------------- BASE DE PROMESAS --------------------------------------------------------------

Promesas <- gestiones_periodo %>% 
  filter(RESPUESTA == "PROMESA_DE_PAGO")


#--------------------------------------------------------- CONSULTA DE PAGOS -------------------------------------------------------

# lista para almacenar los resultados
resultados <- list()

for (ids in id_chunks) {
  id_string <- paste(ids, collapse = ", ") # Convertir el bloque de IDs en una cadena separada por comas
  
  query <- glue(
    "SELECT 
      ASO_IDENTIFICACION_ASOCIADO,
      RCDO_NUMERO_PAGARE,
      CON_CODIGO,
      HRCD_FECHA_RECAUDO,
      HRCD_VALOR_RECAUDO,
      hrcd_periodo_FACTURACION
    FROM GRC.GCC_HISTORIA_RECAUDO
    WHERE HRCD_FECHA_RECAUDO BETWEEN TO_DATE('01/12/2024', 'DD/MM/YYYY')AND TO_DATE('31/12/2024', 'DD/MM/YYYY')
     AND CON_CODIGO IN ('AUT PESA',
                     'AUTLIB',
                     'AUTLIV',
                     'FLE.HOG+',
                     'HOGMASBA',
                     'RC MED',
                     'SEG HOGA',
                     'SEG MOTO',
                     'SEG.VEH.',
                     'SEGAUTOS',
                     'SEGVEHBA',
                     'SEGVEHCB',
                     'VIDASOLI',
                     'TOHOMLIV',
                     'TOHOMLIF')"
  )
  
  
  resultados[[length(resultados) + 1]] <- dbGetQuery(Connecion_SQL, query) # Ejecuta la consulta y almacena el resultado en la lista
}

Base_Recaudos <- bind_rows(resultados) %>% 
  distinct() %>% 
  rename(CEDULA = ASO_IDENTIFICACION_ASOCIADO, PAGARE = RCDO_NUMERO_PAGARE, RECAUDO = HRCD_VALOR_RECAUDO, 
         FECHA_RECAUDO = HRCD_FECHA_RECAUDO) %>% 
  group_by(CEDULA, PAGARE) %>%
  summarise(
    Q_RECAUDO = n(),
    FECHA_RECAUDO = max(FECHA_RECAUDO, na.rm = TRUE),
    RECAUDO = sum(RECAUDO, na.rm = TRUE),
  ) %>% 
  mutate(PAGARE = as.numeric(PAGARE))


#--------------------------------------------------------- CONSULTA DE FACTURACION ----------------------------------------------------

id_chunks2 <- split(cedulas_polizas$CEDULA, ceiling(seq_along(cedulas_polizas$CEDULA) / 1000)) # Divide en bloques de 1000


# lista para almacenar los resultados
resultados2 <- list()

for (ids2 in id_chunks2) {
  id_string2 <- paste(ids2, collapse = ", ") # Convertir el bloque de IDs en una cadena separada por comas
  
  query2 <- glue(
    "SELECT ASO_IDENTIFICACION,
  CON_CODIGO,
  HFR_NUMERO_PAGARE,
  HFR_PERIODO_FACT,
  HFR_VALOR_CUOTA_MES_CALCULADO,
  HFR_VALOR_VENCIDO_CALCULADO,
  HFR_VALOR_CUOTA_MES_CALCULADO + HFR_VALOR_VENCIDO_CALCULADO as FACTURADO_TOTAL
  FROM grc.gcc_hist_fact_recaudo
  WHERE HFR_PERIODO_FACT = 202412
  AND CON_CODIGO IN ('AUT PESA',
                     'AUTLIB',
                     'AUTLIV',
                     'FLE.HOG+',
                     'HOGMASBA',
                     'RC MED',
                     'SEG HOGA',
                     'SEG MOTO',
                     'SEG.VEH.',
                     'SEGAUTOS',
                     'SEGVEHBA',
                     'SEGVEHCB',
                     'VIDASOLI',
                     'TOHOMLIV',
                     'TOHOMLIF');"
  )
  
  
  resultados2[[length(resultados2) + 1]] <- dbGetQuery(Connecion_SQL, query2) # Ejecuta la consulta y almacena el resultado en la lista
}

Base_Facturacion <- bind_rows(resultados2)

dbDisconnect(Connecion_SQL)


#------------------------------------------------- FACTURACION------------------------------------------------------

Facturacion <- Base_Facturacion %>% 
  rename(CEDULA = ASO_IDENTIFICACION, PAGARE = HFR_NUMERO_PAGARE) %>% 
  filter(HFR_PERIODO_FACT == 202412) %>%  #<------------- cambiar por periodo actual
  mutate(PAGARE = as.numeric(PAGARE)) %>% 
  distinct()



#--------------------------------------------------- SEGUIMIENTO PROMESAS -----------------------------------------------

#SEGUIMIENTO PROMESAS MES ANTERIOR
ruta_archivo <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/2024/Bases_Cruce/Promesas_Mora35_112024.xlsx"
nombre_hoja <- "Promesas"
Polizas_periodo_anterior  <- read_excel(ruta_archivo, sheet = nombre_hoja) %>% 
  clean_names(case="all_caps") %>% 
  filter(ESTADO_PROMESA == "PENDIENTE") %>% 
  distinct(CEDULA, CON_CODIGO, PAGARE, USUARIO_MANUAL, FECHA_GESTION, FECHA_PROMESA_MANUAL) %>% 
  rename(FECHA_PROMESA = FECHA_PROMESA_MANUAL, USUARIO = USUARIO_MANUAL) %>% 
  mutate(FECHA_GESTION = as.character(FECHA_GESTION),
         FECHA_PROMESA = as.character(FECHA_PROMESA)) %>% 
  select(CEDULA, CON_CODIGO, PAGARE, FECHA_GESTION, FECHA_PROMESA, USUARIO)

#PROMESAS ACTUALES
polizas_acuerdo <- Consolidado_Polizas %>%
  inner_join( Promesas %>% select(CEDULA, FECHA_GESTION, USUARIO, ACCION, RESPUESTA, FECHA_PROMESA, VALOR_PROMESA)) %>%
  arrange(CEDULA, CON_CODIGO, PAGARE, desc(FECHA_GESTION)) %>% 
  distinct(CEDULA, CON_CODIGO, PAGARE, .keep_all = TRUE) 

#TOTAL PROMESAS
promesas_polizas <- bind_rows(Polizas_periodo_anterior, polizas_acuerdo) %>% 
  left_join(Facturacion %>%  select(CEDULA, PAGARE, HFR_VALOR_CUOTA_MES_CALCULADO, HFR_VALOR_VENCIDO_CALCULADO), 
            by = c("CEDULA" = "CEDULA", "PAGARE" = "PAGARE")) %>% 
  left_join(Base_Recaudos %>%  select(CEDULA, PAGARE, Q_RECAUDO, FECHA_RECAUDO, RECAUDO), 
            by = c("CEDULA" = "CEDULA", "PAGARE" = "PAGARE")) %>% 
  arrange(CEDULA, CON_CODIGO, PAGARE, desc(FECHA_GESTION)) %>% 
  distinct(CEDULA, CON_CODIGO, PAGARE, .keep_all = TRUE) 

#CREACION ARCHIVO
promesas_polizas %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/2024/Bases_Cruce/SPP_202412.xlsx",
      col_names=TRUE,sheet="Hoja1"))

#-------------------------------------------------- SEGUIMIENTO GESTION ------------------------------------------------

gestion_polizas <- datos_polizas %>%
  mutate(CEDULA = as.numeric(CEDULA)) %>% 
  left_join( gestiones_periodo %>% select(CEDULA, FECHA_GESTION, CASA_COBRANZA, USUARIO, ACCION, RESPUESTA, FECHA_PROMESA, VALOR_PROMESA), 
             by = c("CEDULA" = "CEDULA")) %>% 
  select(TIPO_DOCUMENTO, CEDULA, NOMBRE, CON_CODIGO, PAGARE, REGIONAL, CORTE, DIA_CORTE, ESTADO, DIAS_DE_MORA, NUMERO_CUOTAS_ATRASADAS, 
         FECHA_GESTION, CASA_COBRANZA, USUARIO, ACCION, RESPUESTA, FECHA_PROMESA, VALOR_PROMESA)

#CREACION ARCHIVO
gestion_polizas %>%
  write_xlsx(
    glue(
      "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Mora_35/2024/Bases_Cruce/STP_202412.xlsx",
      col_names=TRUE,sheet="Hoja1"))

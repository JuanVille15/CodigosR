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
 
## Se lee la base de Empleados ##

ruta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/GCC/Planta Coomeva"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

## Darle Formato ##

data <- data %>% select(`ID Emp`,Empresa,`Id Regional`,Regional,`ID Departamento`,Departamento,`ID Unidad Organizacional`,`Unidad Organizacional`,
                        `Id Lugar de Trabajo`, `Lugar de Trabajo`,`Id Posición`, Posición, `ID Puesto`,Puesto, `ID Jefe`, `Nombre Jefe`,
                        `ID Persona`, `Nombre Completo`, `Tipo de Documento`,`N° Documento de Identidad...33`,`Fecha de Ingreso`,`Fecha ingreso al GECC`,
                        `Fecha ingreso al GECC`)

data <- data %>% mutate( `Abrev Emp ` = "",
                         `ID Municipio` = "",
                        Municipio = "",
                        `Id Sector` = "",
                        Sector = "",
                        `Id Negocio` = "",
                        Negocio = "",
                        `Id Und Org.` = "",
                        `Id Lug. Trabajo` = "",
                        `Usuario Peoplenet` = "",
                         Genero = "",
                        `Estado Civil` = "",
                        `Factor RH` = "",
                        `Fecha Nacimiento` = "",
                        Edad = "",
                        `Direccion Residencia` = "",
                        Barrio = "",
                        `Tel Personal` = "",
                        Profesion = "",
                        Especializacion = "",
                        `C. Costo RH` = "",
                        `C.Costo Oracle` = "",
                        `Cuenta de Correo` = "",
                        `Direccion Oficina` = "")

data <- data %>% rename(`Id Emp.` = `ID Emp`,
                        `Id Regional` = `Id Regional`,
                        `id Depto` = `ID Departamento`,
                        `Unidad Org.` = `Unidad Organizacional`,
                        `Id Posicion` = `Id Posición`,
                        Posicion = Posición,
                        `Id Puesto` = `ID Puesto`,
                        `Puesto de trabajo` = Puesto,
                        `Id Jefe` = `ID Jefe`,
                        `Nombre Jefe Inmediato` = `Nombre Jefe`,
                        `Id Persona` = `ID Persona`,
                        `Nombre Empleado` = `Nombre Completo`,
                        `Tipo Doc` = `Tipo de Documento`,
                        `N° Cedula` = `N° Documento de Identidad...33`,
                        `Fecha Ingreso` = `Fecha de Ingreso`,
                        `Fecha Ingreso GECC` = `Fecha ingreso al GECC`)

data <- data %>% mutate(Ord. = "", 
                        `Fecha Inicio` = `Fecha Ingreso GECC`,
                        `Abrev Emp` = "")

## Organizar el documento ##

data <- data %>% select(`Id Emp.`,
                         Empresa,
                        `Abrev Emp`,
                        `Id Regional`,
                         Regional,
                        `id Depto`,
                         Departamento,
                        `ID Municipio`,
                         Municipio,
                        `Id Sector`,
                         Sector,
                        `Id Negocio`,
                         Negocio,
                        `Id Und Org.`,
                        `Unidad Org.`,
                        `Id Lug. Trabajo`,
                        `Lugar de Trabajo`,
                        `Id Posicion`,
                         Posicion,
                        `Id Puesto`,
                        `Puesto de trabajo`,
                        `Id Jefe`,
                        `Nombre Jefe Inmediato`,
                        `Id Persona`,
                         Ord.,
                        `Nombre Empleado`,
                        `Usuario Peoplenet`,
                         Genero,
                        `Estado Civil`,
                        `Factor RH`,
                        `Tipo Doc`,
                        `N° Cedula`,
                        `Fecha Nacimiento`,
                         Edad,
                        `Direccion Residencia`,
                         Barrio,
                        `Tel Personal`,
                         Profesion,
                         Especializacion,
                        `Fecha Ingreso`,
                        `Fecha Ingreso GECC`,
                        `Fecha Inicio`,
                        `C. Costo RH`,
                        `C.Costo Oracle`,
                        `Cuenta de Correo`,
                        `Direccion Oficina`)

data <- data %>% mutate(`Fecha Ingreso` = format(as.Date(`Fecha Ingreso`), "%d/%m/%Y"),
                        `Fecha Ingreso GECC` = format(as.Date(`Fecha Ingreso GECC`), "%d/%m/%Y"),
                        `Fecha Inicio` = format(as.Date(`Fecha Inicio`), "%d/%m/%Y"))

write_xlsx(data, "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/BD SORTEO ESTAR ALDIA TE PREMIA/extrPlanoColaboradoresPagoOportunoJunio2025.xlsx")                        

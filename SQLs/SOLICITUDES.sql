INSERT INTO   ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt 
( 
	PERIODO, rgst_nro_solicitud, ORIGEN_TRANSACCIONALIDAD, FECHA_REGISTRO, HORA_REGISTRO, 
	FECHA_ASIGNACION_PERFIL, HORA_ASIGNACION_PERFIL
	
)

SELECT  

OPE.PERIODO AS PERIODO, 
OPE.nro_id_sesion  AS rgst_nro_solicitud, 
MAX(CASE WHEN OPE.cod_servicio='ConsultaPerfil-Cloud' THEN  'Administracion Perfil' END) AS ORIGEN_TRANSACCIONALIDAD, 
MAX(CASE WHEN OPE.cod_servicio='GeneracionPefil-Cloud' THEN  from_timestamp(OPE.FECHA_HORA_OPERACIOn, 'yyyy-MM-dd') END) AS FECHA_REGISTRO, 
MAX(CASE WHEN OPE.cod_servicio='GeneracionPefil-Cloud' THEN  CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING) END) AS HORA_REGISTRO,
MAX(CASE WHEN OPE.cod_servicio='GeneracionPefil-Cloud' THEN  from_timestamp(OPE.FECHA_HORA_OPERACIOn, 'yyyy-MM-dd') END) AS FECHA_ASIGNACION_PERFIL, 
MAX(CASE WHEN OPE.cod_servicio='GeneracionPefil-Cloud' THEN  CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING) END) AS HORA_ASIGNACION_PERFIL

FROM ${var:bd_cruda}.mbaas_log_operacional OPE

WHERE OPE.ID_APLICACION IN ('EPR')    
AND OPE.PERIODO  = ${var:periodo_inicial}
GROUP BY  OPE.PERIODO,  OPE.NRO_ID_SESION ;
 

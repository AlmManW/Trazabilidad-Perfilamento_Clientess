UPDATE S 

SET  
S.EXONERACION_RESPONSABILIDAD = T.EXONERACION_RESPONSABILIDAD

FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S 

JOIN (
		SELECT 
		
		MAX(CASE WHEN JUR.tipo_documento ='exoneracionDeResponsabilidad'  THEN 'Si' ELSE 'No' END) AS EXONERACION_RESPONSABILIDAD,
		
		JUR.nro_id_sesion

		FROM ${var:bd_cruda}.mbaas_log_juridico JUR  
		WHERE ID_APLICACION IN ('FIC')
		AND PERIODO = ${var:periodo_inicial}
		GROUP BY nro_id_sesion
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;
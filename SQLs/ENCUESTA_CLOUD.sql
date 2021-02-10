UPDATE S 
SET  
S.CANT_ENCUESTA_REALIZADA = T.CANT_ENCUESTA_REALIZADA,
S.NRO_PROCESO_CULMINADO = T.NRO_PROCESO_CULMINADO

FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S 

JOIN (
		SELECT 
		PER.cant_intento_encuesta_sal AS CANT_ENCUESTA_REALIZADA, 
		'2' AS NRO_PROCESO_CULMINADO,
		PER.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt PER  
		WHERE ID_APLICACION IN ('EPR')
		AND PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(PER.NRO_ID_SESION, CAST(PER.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;


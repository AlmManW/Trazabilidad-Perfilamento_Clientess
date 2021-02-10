UPDATE S 
SET  
 
S.FECHA_VENCIMIENTO_PERFIL = T.FECHA_VENCIMIENTO_PERFIL,
S.VERSION_ENCUESTA_DILIGENCIADA = T.VERSION_ENCUESTA_DILIGENCIADA,
S.USUARIO_REGISTRO = T.USUARIO_REGISTRO, 
S.TEXTO_PREGUNTA_1 = T.TEXTO_PREGUNTA_1, 
S.TEXTO_PREGUNTA_2 = T.TEXTO_PREGUNTA_2, 
S.TEXTO_PREGUNTA_3 = T.TEXTO_PREGUNTA_3, 
S.TEXTO_PREGUNTA_4 = T.TEXTO_PREGUNTA_4, 
S.TEXTO_PREGUNTA_5 = T.TEXTO_PREGUNTA_5, 
S.TEXTO_PREGUNTA_6 = T.TEXTO_PREGUNTA_6, 
S.TEXTO_PREGUNTA_7 = T.TEXTO_PREGUNTA_7, 
S.TEXTO_PREGUNTA_8 = T.TEXTO_PREGUNTA_8, 
S.TEXTO_PREGUNTA_9 = T.TEXTO_PREGUNTA_9, 
S.TEXTO_PREGUNTA_10 = T.TEXTO_PREGUNTA_10,
S.NRO_PROCESO_CULMINADO = T.NRO_PROCESO_CULMINADO, 
S.CANT_ENCUESTA_REALIZADA = S.CANT_ENCUESTA_REALIZADA + T.valor

FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S

JOIN (
		SELECT

		
		from_timestamp(PER.fecha_vencimiento_sal, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO_PERFIL,
		PER.version_ent AS VERSION_ENCUESTA_DILIGENCIADA,
		PER.nro_id_usuario_ent AS USUARIO_REGISTRO,

		replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 1), '[', '') AS TEXTO_PREGUNTA_1,
		SPLIT_PART(PER.descripcion_pregunta_ent,',', 2) AS TEXTO_PREGUNTA_2,
		SPLIT_PART(PER.descripcion_pregunta_ent,',', 3) AS TEXTO_PREGUNTA_3,
		SPLIT_PART(PER.descripcion_pregunta_ent,',', 4) AS TEXTO_PREGUNTA_4,
		replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 5), ']', '') AS TEXTO_PREGUNTA_5,
 		replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 6), ']', '') AS TEXTO_PREGUNTA_6,
    replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 7), ']', '') AS TEXTO_PREGUNTA_7,
    replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 8), ']', '') AS TEXTO_PREGUNTA_8,
    replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 9), ']', '') AS TEXTO_PREGUNTA_9,
 		replace(SPLIT_PART(PER.descripcion_pregunta_ent,',', 10), ']', '') AS TEXTO_PREGUNTA_10,  
		'3' AS NRO_PROCESO_CULMINADO,
   CASE WHEN PER.nombre_perfil_riesgo_sal IS NOT NULL OR PER.nombre_perfil_riesgo_sal != '' 
			THEN  1 else 0
			END AS valor, 
		PER.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt PER  
		WHERE ID_APLICACION IN ('EPR')
		AND PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(PER.NRO_ID_SESION, CAST(PER.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;




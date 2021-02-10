UPDATE S 
SET  
S.ORIGEN_ADMINISTRACION_PERFIL = T.ORIGEN_ADMINISTRACION_PERFIL, 
S.PERFIL_ANTERIOR_CLIENTE = T.PERFIL_ANTERIOR_CLIENTE,
S.FECHA_ANTERIOR_PERFIL = T.FECHA_ANTERIOR_PERFIL,
S.FECHA_VENCIMIENTO_ANTERIOR_PERFIL = T.FECHA_VENCIMIENTO_ANTERIOR_PERFIL, 
S.FLG_TIPO_CLIENTE_INVERSIONISTA = T.FLG_TIPO_CLIENTE_INVERSIONISTA, 
S.ORIGEN_PROCESO = T.ORIGEN_PROCESO, 
S.HORA_ANTERIOR_PERFIL = T.HORA_ANTERIOR_PERFIL, 
S.NRO_PROCESO_CULMINADO = T.NRO_PROCESO_CULMINADO,
S.ORIGEN_CONTRATACION = T.ORIGEN_CONTRATACION



FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S 

JOIN (
		SELECT 

		PER.estado_perfilamiento_cliente_sal AS ORIGEN_ADMINISTRACION_PERFIL,
		PER.flg_tiene_perfilamiento_sal AS PERFIL_ANTERIOR_CLIENTE,
		TRUNC(PER.fecha_perfil_sal, 'dd') AS FECHA_ANTERIOR_PERFIL, 
		from_timestamp(PER.fecha_perfil_sal, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO_ANTERIOR_PERFIL,
		PER.flg_tipo_inversionista_sal AS FLG_TIPO_CLIENTE_INVERSIONISTA, 
		CASE WHEN PER.cod_producto_sal = '' OR PER.cod_producto_sal IS NULL THEN 'Administración de perfil' ELSE 'Contratación de producto' END AS ORIGEN_PROCESO, 
		CAST(from_timestamp(PER.fecha_perfil_sal,'HH:mm:ss') AS STRING) AS HORA_ANTERIOR_PERFIL, 
		'1' AS NRO_PROCESO_CULMINADO,
		CASE WHEN PER.cod_producto_ent = '' OR PER.cod_producto_ent IS NULL THEN PER.cod_producto_ent ELSE 'Fondos de Inversion ' END AS ORIGEN_CONTRATACION,
		PER.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_consulta_perfil_riesgo_nrt PER  
		WHERE ID_APLICACION IN ('EPR')
		AND PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora perración

        AND (CONCAT(PER.NRO_ID_SESION, CAST(PER.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_consulta_perfil_riesgo_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;









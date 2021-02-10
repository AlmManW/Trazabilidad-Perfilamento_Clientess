UPDATE S 
SET  
S.COD_TIPO_IDENTIFICACION_CLIENTE = T.COD_TIPO_IDENTIFICACION_CLIENTE, 
S.NRO_IDENTIFICACION_CLIENTE = T.NRO_IDENTIFICACION_CLIENTE,
S.NOMBRE_CLIENTE = T.NOMBRE_CLIENTE,
S.PRIMER_APELLIDO_CLIENTE = T.PRIMER_APELLIDO_CLIENTE, 
S.SEGUNDO_APELLIDO_CLIENTE = T.SEGUNDO_APELLIDO_CLIENTE,
S.COD_CANAL_CLIENTE = T.COD_CANAL_CLIENTE, 
S.FECHA_INTERACCION = T.FECHA_INTERACCION,
S.HORA_INTERACCION = T.HORA_INTERACCION,
S.FECHA_PROCESO = T.FECHA_PROCESO,
S.HORA_PROCESO = T.HORA_PROCESO,
S.DIRECCION_IP = T.DIRECCION_IP



FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S 

JOIN (
		SELECT  T.nro_id_sesioN, MAX(T.NOMBRE_OPERACION_ENT) NOMBRE_OPERACION_ENT ,max(T.COD_TIPO_IDENTIFICACION_CLIENTE) COD_TIPO_IDENTIFICACION_CLIENTE,
		max(T.NRO_IDENTIFICACION_CLIENTE) NRO_IDENTIFICACION_CLIENTE,max(T.NOMBRE_CLIENTE) NOMBRE_CLIENTE, 
		max(T.PRIMER_APELLIDO_CLIENTE) PRIMER_APELLIDO_CLIENTE, max(T.SEGUNDO_APELLIDO_CLIENTE) SEGUNDO_APELLIDO_CLIENTE, 
		max(T.COD_CANAL_CLIENTE) COD_CANAL_CLIENTE, max(T.FECHA_INTERACCION) FECHA_INTERACCION, 
		max(T.HORA_INTERACCION) HORA_INTERACCION, max(T.FECHA_PROCESO) FECHA_PROCESO, MAX(T.HORA_PROCESO) HORA_PROCESO, max(T.DIRECCION_IP) DIRECCION_IP,
		max(T.FECHA_HORA_OPERACION)
FROM (

	  SELECT 
        PER.NOMBRE_OPERACION_ENT AS NOMBRE_OPERACION_ENT, 
		PER.cod_tipo_identificacion_ent AS COD_TIPO_IDENTIFICACION_CLIENTE, 
		PER.nro_identificacion_ent  AS NRO_IDENTIFICACION_CLIENTE, 
		PER.nombre_cliente_ent AS NOMBRE_CLIENTE, 
		SPLIT_PART(PER.apellido_cliente_ent, ' ' , 1) AS PRIMER_APELLIDO_CLIENTE, 
		SPLIT_PART(PER.apellido_cliente_ent, ' ' , 2) AS SEGUNDO_APELLIDO_CLIENTE, 

		CASE 
		WHEN PER.cod_canal_peticion_ent='16' THEN 'Davivcom' 
		WHEN PER.nro_oficina_ent = '4884' THEN 'App Davivienda' 
		WHEN PER.nro_oficina_ent = '3092' THEN 'Drupal'
		WHEN PER.nro_oficina_ent = '3093' THEN 'Corredores.com'
		WHEN PER.nro_oficina_ent = '4900' THEN 'App Inversiones '
		ELSE concat('NO DEFINIDO ', PER.cod_canal_peticion_ent, ' y ', PER.nro_oficina_ent) 
		END AS COD_CANAL_CLIENTE, 
		PER.fecha_solicitud_ent AS FECHA_INTERACCION,	 
		CAST(PER.hora_solicitud_ent AS STRING) AS HORA_INTERACCION, 
		PER.fecha_solicitud_ent AS FECHA_PROCESO, 
		CAST(PER.hora_solicitud_ent AS STRING) AS HORA_PROCESO,
		PER.direccion_ip_peticion_ent AS DIRECCION_IP, 
		PER.nro_id_sesion AS nro_id_sesion, PER.fecha_hora_operacion AS fecha_hora_operacion

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

    UNION ALL 

      SELECT 
        PER.NOMBRE_OPERACION_ENT AS NOMBRE_OPERACION_ENT, 
		PER.cod_tipo_identificacion_ent AS COD_TIPO_IDENTIFICACION_CLIENTE, 
		PER.nro_identificacion_cliente_ent  AS NRO_IDENTIFICACION_CLIENTE, 
		PER.nombres_cliente_ent AS NOMBRE_CLIENTE, 
		SPLIT_PART(PER.apellidos_cliente_ent, ' ' , 1) AS PRIMER_APELLIDO_CLIENTE, 
		SPLIT_PART(PER.apellidos_cliente_ent, ' ' , 2) AS SEGUNDO_APELLIDO_CLIENTE, 
		
		CASE 
		WHEN PER.cod_canal_peticion_ent='16' THEN 'Davivcom' 
		WHEN PER.cod_oficina_ent = '4884' THEN 'App Davivienda' 
		WHEN PER.cod_oficina_ent = '3092' THEN 'Drupal'
		WHEN PER.cod_oficina_ent = '3093' THEN 'Corredores.com'
		WHEN PER.cod_oficina_ent = '4900' THEN 'App Inversiones '
		ELSE concat('NO DEFINIDO ', PER.cod_canal_peticion_ent, ' y ', PER.cod_oficina_ent) 
		END AS COD_CANAL_CLIENTE, 

		
		PER.fecha_solicitud_ent AS FECHA_INTERACCION,	 
		CAST(PER.hora_solicitud_ent AS STRING) AS HORA_INTERACCION, 
		PER.fecha_solicitud_ent AS FECHA_PROCESO, 
		CAST(PER.hora_solicitud_ent AS STRING) AS HORA_PROCESO,
		PER.direccion_ip_origen_ent AS DIRECCION_IP, 
		--PER.cant_intento_encuesta_sal AS CANT_ENCUESTA_REALIZADA, 
-- 	
		PER.nro_id_sesion AS nro_id_sesion, PER.fecha_hora_operacion AS fecha_hora_operacion

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

GROUP BY T.nro_id_sesion


	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;






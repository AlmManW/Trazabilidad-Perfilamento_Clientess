UPDATE S 
SET  
S.TEXTO_RESPUESTA_1 = T.TEXTO_RESPUESTA_1, 
S.TEXTO_RESPUESTA_2 = T.TEXTO_RESPUESTA_2, 
S.TEXTO_RESPUESTA_3 = T.TEXTO_RESPUESTA_3, 
S.TEXTO_RESPUESTA_4 = T.TEXTO_RESPUESTA_4, 
S.TEXTO_RESPUESTA_5 = T.TEXTO_RESPUESTA_5, 
S.TEXTO_RESPUESTA_6 = T.TEXTO_RESPUESTA_6, 
S.TEXTO_RESPUESTA_7 = T.TEXTO_RESPUESTA_7, 
S.TEXTO_RESPUESTA_8 = T.TEXTO_RESPUESTA_8, 
S.TEXTO_RESPUESTA_9 = T.TEXTO_RESPUESTA_9, 
S.TEXTO_RESPUESTA_10 = T.TEXTO_RESPUESTA_10

FROM ${var:bd_temp}.temp_perfilamiento_trazabilidad_nrt S 

JOIN (
		

SELECT P.id1 as nro_id_sesion, P.id2, 

CASE WHEN P.idOpcion1 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion1,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE '' 
END AS TEXTO_RESPUESTA_1,

CASE WHEN P.idOpcion2 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion2,translate(PE.nro_opcion_respuesta_sal,'[] ','')))
ELSE ''
END AS TEXTO_RESPUESTA_2,

CASE WHEN P.idOpcion3 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion3,translate(PE.nro_opcion_respuesta_sal,'[] ','')))
ELSE ''
END AS TEXTO_RESPUESTA_3,

CASE WHEN P.idOpcion4 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion4,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE ''
END AS TEXTO_RESPUESTA_4,

CASE WHEN P.idOpcion5 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion5,translate(PE.nro_opcion_respuesta_sal,'[] ','')))
ELSE ''
END AS TEXTO_RESPUESTA_5,

CASE WHEN P.idOpcion6 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion6,translate(PE.nro_opcion_respuesta_sal,'[] ','')))
ELSE ''
END AS TEXTO_RESPUESTA_6,

CASE WHEN P.idOpcion7 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion7,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE ''
END AS TEXTO_RESPUESTA_7,

CASE WHEN P.idOpcion8 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion8,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE '' 
END AS TEXTO_RESPUESTA_8,

CASE WHEN P.idOpcion9 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion9,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE ''
END AS TEXTO_RESPUESTA_9,

CASE WHEN P.idOpcion10 != ''
THEN SPLIT_PART(regexp_replace(translate(PE.texto_apoyo_opcion_respuesta_sal,'[]',''),'\\.?,\\s+([A-Z])','- \\1'),'-',find_in_set(P.idOpcion10,translate(PE.nro_opcion_respuesta_sal,'[] ',''))) 
ELSE '' 
END AS TEXTO_RESPUESTA_10

from (
SELECT PE.nro_id_sesion as id1, per.nro_id_sesion as id2, 
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',1) as idOpcion1, 
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',2) as idOpcion2,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',3) as idOpcion3,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',4) as idOpcion4,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',5) as idOpcion5, 
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',6) as idOpcion6, 
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',7) as idOpcion7,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',8) as idOpcion8,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',9) as idOpcion9,
SPLIT_PART(translate(PER.nro_respuesta_ent,'[] ',''),',',10) as idOpcion10

from ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt PE left join  ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt PER 
using (nro_id_sesion)
WHERE PE.ID_APLICACION IN ('EPR')
AND PE.PERIODO = ${var:periodo_inicial} 
)P JOIN  ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt PE ON P.id1 = PE.nro_id_sesion

	--Subconsulta para garantizar la MAX fecha hora operación
WHERE PE.ID_APLICACION IN ('EPR')
AND PE.PERIODO = ${var:periodo_inicial} 
        AND (CONCAT(PE.NRO_ID_SESION, CAST(PE.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial} 
            GROUP BY T.nro_id_sesion
            )
) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;
SELECT oco.numero_ocorrencia,
                      oco.qtd_ocorrencia,
                      oco.natureza_descricao,
                      oco.natureza_consumado,
                      CAST (oco.data_hora_fato as date) as data_fato,
                      YEAR (oco.data_hora_fato) as ano_fato,
                      MONTH (oco.data_hora_fato) as mes_numerico_fato,
                      SUBSTRING(CAST(oco.data_hora_fato AS STRING), 12, 8) AS horario_fato,
                      oco.motivo_presumido_descricao_longa,
                      oco.instrumento_utilizado_descricao_longa,
                      oco.local_imediato_longa,
                      oco.complemento_natureza_descricao,
                      oco.tipo_logradouro_descricao,
                      oco.descricao_endereco,
                      oco.nome_bairro,
                      oco.nome_municipio,
                      oco.codigo_municipio,
                      oco.ocorrencia_uf,
                      oco.unidade_responsavel_registro_nome,
                      oco.numero_latitude,
                      oco.numero_longitude,
                      oco.tipo_local_descricao,
                      oco.natureza_ind_consumado
               FROM db_bisp_reds_reporting.tb_ocorrencia AS oco
               WHERE oco.data_hora_fato >= '2020-01-01 00:00:00.000'
               AND oco.data_hora_fato < '2025-01-01 00:00:00.000'
               AND oco.ocorrencia_uf = 'MG'
               AND oco.ind_estado IN ('F', 'R')
               AND oco.natureza_codigo IN ('U33024', 'D08064', 'D08640', 'M31015', 'M31527', 'M31014', 'M31016', 'M30032')

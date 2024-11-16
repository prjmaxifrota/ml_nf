WITH trn (
    tipoequipamento, 
    idfamilia,
    familia,
    idmodelo,
    modelo, 
    combustivel, 
    idcliente,
    cliente,  
    idcondutor,
    condutor, 
    cidade, 
    estado,  
    idestabelecimento,
    estabelecimento, 
    data, 
    ano,
    mes,
    dia,
    dia_semana,
    momento_dia,
    anofabricacao,
    anofabricacaocorreto,
    idade_equipamento,
    uso_km_horas,
    anomodelo,
    anomodelocorreto,
    grupo,  
    dist_melhoropcao_cidade,  
    variacao_tipo,
    centrocusto, 
    latitude,  
    longitude,  
    km_rodado,  
    km_atual,   
    km_ajustado, 
    km_rodado_efetivo,
    km_ajustado_menor, 
    km_ajuste, 
    custo_km, 
    qtde,     
    qtde_ajustada,
    qtde_efetiva,
    qtde_ajustado_menor,
    qtde_ajuste, 
    valor_total, 
    valor_unit,  
    min_vlunit,  
    med_vlunit,  
    valor_unit_ajustado,
    valor_unit_efetivo,
    valor_unit_ajustado_menor,
    valor_unit_ajuste, 
    econ_realizada,    
    econ_potencial, 
    econ_menor_preco,  
    custo_econ,   
    econ_realizada_menor,
    consumotipo,
    consumo,
    consumo_ajustado,
    consumomodelo, 
    consumo_efetivo,
    consumo_ajustado_menor,
    consumo_ajuste,
    consumo_maior_modelo,
    idtransacaoext,  
    idtransacao
) AS (
SELECT 
    te.tipoequipamento, 
    t.idfamilia,
    f.familia,
    t.idmodelo,
    m.modelo, 
    t.combustivel, 
    t.idcliente,
    cl.cliente,  
    t.idcondutor,
    cn.condutor, 
    t.cidade, 
    t.estado,  
    t.idestabelecimento,
    es.estabelecimento, 
    t.data, 
    YEAR(t.data) AS ano,
    MONTH(t.data) AS mes,
    DAY(t.data) AS dia,
    CASE DATEPART(dw, t.data) 
        WHEN 1 THEN 'Domingo' 
        WHEN 2 THEN 'Segunda'
        WHEN 3 THEN 'Terça'
        WHEN 4 THEN 'Quarta'
        WHEN 5 THEN 'Quinta'
        WHEN 6 THEN 'Sexta'
        WHEN 7 THEN 'Sábado'
    END AS dia_semana,
    CASE 
        WHEN DATEPART(HOUR, t.datahora) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN DATEPART(HOUR, t.datahora) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN DATEPART(HOUR, t.datahora) BETWEEN 18 AND 22 THEN 'Noite'
        ELSE 'Tarde da noite'
    END AS momento_dia,
    CASE WHEN e.anofabricacao IS NULL THEN 0 ELSE e.anofabricacao END AS anofabricacao,
    CASE WHEN LEN(LTRIM(RTRIM(CAST(e.anofabricacao AS VARCHAR(4))))) = 4 THEN 1 ELSE 0 END AS anofabricacaocorreto,
    CASE WHEN LEN(LTRIM(RTRIM(CAST(e.anofabricacao AS VARCHAR(4))))) = 4 
         THEN YEAR(GETDATE()) - e.anofabricacao
         ELSE -1
    END AS idade_equipamento,
    CASE 
        -- Veiculo/Equipamento Uso KM/Horas de Uso
        
        -- veículos leves e suvs
        WHEN LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%automovel%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%veiculo leve%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%carro%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%sedan%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%suv%' THEN
            CASE
                WHEN km_atual <= 5000 THEN 'zero'
                WHEN km_atual <= 30000 THEN 'novo'
                WHEN km_atual <= 70000 THEN 'semi-novo'
                WHEN km_atual <= 150000 THEN 'usado'
                ELSE 'velho (rodado)'
            END
        
        -- motocicletas e motos
        WHEN LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%motocicleta%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%moto%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%motociclo%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%motoneta%' THEN
            CASE
                WHEN km_atual <= 3000 THEN 'zero'
                WHEN km_atual <= 15000 THEN 'novo'
                WHEN km_atual <= 30000 THEN 'semi-novo'
                WHEN km_atual <= 60000 THEN 'usado'
                ELSE 'velho (rodado)'
            END
        
        -- caminhões e veículos pesados
        WHEN LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%caminhao%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%cavalo%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%carreta%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%caminhonete%' THEN
            CASE
                WHEN km_atual <= 10000 THEN 'zero'
                WHEN km_atual <= 60000 THEN 'novo'
                WHEN km_atual <= 120000 THEN 'semi-novo'
                WHEN km_atual <= 250000 THEN 'usado'
                ELSE 'velho (rodado)'
            END

        -- tratores e equipamentos agrícolas
        WHEN LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%trator%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%retroescavadeira%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%pá carregadeira%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%colheitadeira%' THEN
            CASE
                WHEN km_atual <= 1000 THEN 'zero'
                WHEN km_atual <= 5000 THEN 'novo'
                WHEN km_atual <= 15000 THEN 'semi-novo'
                WHEN km_atual <= 30000 THEN 'usado'
                ELSE 'velho (rodado)'
            END

        -- veículos aquáticos
        WHEN LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%lancha%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%barco%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%bote%' 
            OR LOWER(tipoequipamento COLLATE Latin1_General_CI_AI) LIKE '%jet ski%' THEN
            CASE
                WHEN km_atual <= 100 THEN 'zero'
                WHEN km_atual <= 500 THEN 'novo'
                WHEN km_atual <= 1500 THEN 'semi-novo'
                WHEN km_atual <= 3000 THEN 'usado'
                ELSE 'velho (rodado)'
            END
        -- outros tipos desconhecidos
        ELSE 'tipo desconhecido'
    END AS uso_km_horas,
    CASE WHEN e.anomodelo IS NULL THEN 0 ELSE e.anomodelo END AS anomodelo,
    CASE WHEN LEN(LTRIM(RTRIM(CAST(e.anomodelo AS VARCHAR(4))))) = 4 THEN 1 ELSE 0 END AS anomodelocorreto,
    CASE WHEN t.grupo IS NOT NULL THEN t.grupo ELSE '0' END AS grupo,  
    CASE WHEN t.dist_melhoropcao_cidade IS NOT NULL THEN t.dist_melhoropcao_cidade ELSE 0.00 END AS dist_melhoropcao_cidade,  
    CASE WHEN t.variacao_tipo IS NOT NULL THEN t.variacao_tipo ELSE 0.00 END AS variacao_tipo,
    CASE WHEN t.centrocusto IS NOT NULL THEN t.centrocusto ELSE '0' END AS centrocusto, 
    CASE WHEN es.latitude IS NOT NULL THEN es.latitude ELSE 0 END AS latitude,  
    CASE WHEN es.longitude IS NOT NULL THEN es.longitude ELSE 0 END AS longitude,  
    CASE WHEN km_rodado IS NOT NULL THEN CAST(km_rodado AS FLOAT) ELSE 0.00 END AS km_rodado,  
    CASE WHEN km_atual IS NOT NULL THEN CAST(km_atual AS FLOAT) ELSE 0.00 END AS km_atual,   
    CASE WHEN km_ajustado IS NOT NULL THEN CAST(km_ajustado AS FLOAT) ELSE 0.00 END AS km_ajustado, 

    CASE WHEN km_ajustado > 0 THEN km_ajustado ELSE km_rodado END AS km_rodado_efetivo,
    CASE WHEN (km_ajustado > 0 AND km_ajustado < km_rodado) THEN 1 ELSE 0 END AS km_ajustado_menor, 
    CASE WHEN (km_ajustado > 0) THEN 1 ELSE 0 END AS km_ajuste, 

    CAST(t.valor AS FLOAT) / 
            CASE WHEN CAST(t.km_ajustado AS FLOAT) > 0.00
            THEN 
                 CAST(t.km_ajustado AS FLOAT)
            ELSE 
                 CASE WHEN CAST(t.km_rodado AS FLOAT) > 0.00 
                 THEN CAST(t.km_rodado AS FLOAT) 
                 ELSE 1 
                 END
            END
    AS custo_km, 
    CASE WHEN qtde IS NOT NULL THEN CAST(qtde AS FLOAT) ELSE 0.00 END AS qtde,     
    CASE WHEN qtde_ajustada IS NOT NULL THEN CAST(qtde_ajustada AS FLOAT) ELSE 0.00 END AS qtde_ajustada,

    CASE WHEN qtde_ajustada > 0 THEN qtde_ajustada ELSE qtde END AS qtde_efetiva,
    CASE WHEN (qtde_ajustada > 0 AND qtde_ajustada < qtde) THEN 1 ELSE 0 END AS qtde_ajustado_menor,
    CASE WHEN (qtde_ajustada > 0) THEN 1 ELSE 0 END AS qtde_ajuste, 

    CASE WHEN valor IS NOT NULL THEN CAST(valor AS FLOAT) ELSE 0.00 END AS valor_total, 

    CASE WHEN valor_unit IS NOT NULL THEN CAST(valor_unit AS FLOAT) ELSE 0.00 END AS valor_unit,  
    CASE WHEN min_vlunit IS NOT NULL THEN CAST(min_vlunit AS FLOAT) ELSE 0.00 END AS min_vlunit,  
    CASE WHEN med_vlunit IS NOT NULL THEN CAST(med_vlunit AS FLOAT) ELSE 0.00 END AS med_vlunit,  
    CASE WHEN valor_unit_ajustado IS NOT NULL THEN CAST(valor_unit_ajustado AS FLOAT) ELSE 0.00 END AS valor_unit_ajustado,
    CASE WHEN valor_unit_ajustado > 0 THEN valor_unit_ajustado ELSE valor_unit END AS valor_unit_efetivo,
    CASE WHEN (valor_unit_ajustado > 0 AND valor_unit_ajustado < valor_unit) THEN 1 ELSE 0 END AS valor_unit_ajustado_menor,
    CASE WHEN (valor_unit_ajustado > 0) THEN 1 ELSE 0 END AS valor_unit_ajuste, 

    CASE WHEN econ_realizada IS NOT NULL THEN CAST(econ_realizada AS FLOAT) ELSE 0.00 END AS econ_realizada,    
    CASE WHEN econ_potencial IS NOT NULL THEN CAST(econ_potencial AS FLOAT) ELSE 0.00 END AS econ_potencial, 
    CASE WHEN econ_menor_preco IS NOT NULL THEN CAST(econ_menor_preco AS FLOAT) ELSE 0.00 END AS econ_menor_preco,  
    CASE WHEN custo_econ IS NOT NULL THEN CAST(custo_econ AS FLOAT) ELSE 0.00 END AS custo_econ,   
    CASE WHEN (econ_realizada > 0 AND econ_realizada > (
                                                             CAST(t.valor AS FLOAT) / 
                                                                  CASE WHEN CAST(t.km_ajustado AS FLOAT) > 0.00
                                                                  THEN 
                                                                       CAST(t.km_ajustado AS FLOAT)
                                                                  ELSE 
                                                                       CASE WHEN CAST(t.km_rodado AS FLOAT) > 0.00 
                                                                       THEN CAST(t.km_rodado AS FLOAT) 
                                                                       ELSE 1 
                                                                       END
                                                                  END  
                                                       )
                ) 
         THEN 1 ELSE 0
    END AS econ_realizada_menor,

    CASE WHEN consumotipo IS NOT NULL THEN CAST(consumotipo AS FLOAT) ELSE 0.00 END AS consumotipo,
    CASE WHEN consumo IS NOT NULL THEN CAST(consumo AS FLOAT) ELSE 0.00 END AS consumo,
    CASE WHEN consumo_ajustado IS NOT NULL THEN CAST(consumo_ajustado AS FLOAT) ELSE 0.00 END AS consumo_ajustado,
    CASE WHEN consumomodelo IS NOT NULL THEN CAST(consumomodelo AS FLOAT) ELSE 0.00 END AS consumomodelo, 
    CASE WHEN consumo_ajustado > 0 THEN consumo_ajustado ELSE consumo END AS consumo_efetivo,
    CASE WHEN (consumo_ajustado > 0 AND consumo_ajustado < consumo) THEN 1 ELSE 0 END AS consumo_ajustado_menor,
    CASE WHEN (consumo_ajustado > 0) THEN 1 ELSE 0 END AS consumo_ajuste,
    CASE WHEN ((consumo_ajustado > consumomodelo) OR (consumo > consumomodelo)) THEN 1 ELSE 0 END AS consumo_maior_modelo,

    CASE WHEN t.idtransacaoext IS NOT NULL THEN t.idtransacaoext ELSE 0 END AS idtransacaoext,  
    t.idtransacao
FROM transacao t 
JOIN equipamento AS e ON e.idequipamento = t.idequipamento
JOIN familia AS f ON f.idfamilia = t.idfamilia 
JOIN modelo AS m ON m.idmodelo = t.idmodelo
JOIN tipoequipamento AS te ON te.idtipoequipamento = e.idtipoequipamento
JOIN cliente AS cl ON cl.idcliente = t.idcliente 
JOIN condutor AS cn ON cn.idcondutor = t.idcondutor AND t.idcliente = cn.idcliente
JOIN estabelecimento AS es ON es.idestabelecimento = t.idestabelecimento )
select top {sample_size} * 
from trn t
where t.anofabricacaocorreto = 1 and t.anomodelocorreto = 1
and anofabricacao > 1900 and anomodelo > 1900 and idade_equipamento > -1
order by data desc 
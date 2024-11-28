SELECT TOP {sample_size}
    t.*,
    s.id AS servico_id,
    s.cod_anp,
    s.nome AS nome_serv,
    cr.*,
	tnf.*,
    nf.*
FROM vw_transacao t
-- Join with vw_credenciado to resolve credenciado_id
INNER JOIN vw_credenciado cr ON cr.id = t.credenciado_id
-- Join with transacao_recolha for transacao_id and nota_fiscal
LEFT JOIN transacao_recolha tnf (NOLOCK) ON tnf.transacao_id = t.id
LEFT JOIN nota_fiscal nf (NOLOCK) ON nf.id = tnf.nota_fiscal_id
-- Join with servico to resolve servico_id
INNER JOIN servico s (NOLOCK) ON s.id = t.servico_id
-- Filters
WHERE 1 = 1
  AND t.versao_aplicativo = 'mobile'
-- Optional filter for a specific credenciado_id
-- AND t.credenciado_id = 73557
ORDER BY t.data_hora_transacao_inicio DESC;
select top {sample_size}
       t.id,
	   t.data_hora_transacao_inicio,
	   t.valor,
	   t.status,
	   t.status_id,
	   --isnull(tc.mcc_id_msg, 5449) mcc,
	   --isnull(tc.codigo_captura_msg, '1234') codigoCaptura,
	   --tc.nome_cred as nome_abrev,
	   --tc.latitude_cred lat_pre,
	   --tc.longitude_cred long_pre, 
	   s.id servico_id,
	   s.cod_anp ,
	   s.nome nome_serv,
	   t.quantidade,
	   tnf.nota_fiscal_id 
  from vw_transacao t 
       inner join vw_credenciado cr on cr.id = t.credenciado_id 
	   left join transacao_recolha tnf (nolock) on tnf.transacao_id = t.id 
	   left join nota_fiscal nf (nolock) on nf.id = tnf.nota_fiscal_id
	   inner join servico s (nolock) on s.id = t.servico_id 
	   --left join transacao_complemento tc on tc.transacao_id = t.id
 where 1=1
   and versao_aplicativo = 'mobile'
create table rec_ml_chave_busca_cred
(codigo_captura varchar(50) not null,
 nome_abrev varchar(50) not null,
 cnpj varchar(20));
 go
alter table rec_ml_chave_busca_cred add constraint  pk_rec_ml_chave_busca_cred primary key (codigo_captura, nome_abrev)
go

create table rec_ml_nota_fiscal 
(id bigint not null primary key,
 chave_nf varchar(100) not null,
 cnpj_fornecedor varchar(20) not null,
 data_emissao datetime not null,
 valor_total money)
go

create table rec_ml_nota_fiscal_item
(id bigint not null primary key,
 nota_fiscal_ml_id bigint not null,
 ncm varchar(100) null,
 descricao varchar(100) null,
 quantidade money,
 valor_unitario money,
 desconto money,
 aliq_icms money,
 valor_icms money,
 valor_total money)
go

create table rec_ml_transacao_nf 
(transacao_id bigint not null,
 nota_fiscal_ml_id bigint not null)
go
 alter table rec_ml_transacao_nf add constraint  pk_transacao_nf_ml primary key (transacao_id, nota_fiscal_ml_id)
 go
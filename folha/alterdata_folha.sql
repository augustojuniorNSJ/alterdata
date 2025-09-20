

-- GRUPOS EMPRESARIAIS


COPY (
SELECT
	cdempresa CODIGO,
	nmempresa RAZAOSOCIAL
FROM wphd.empresa
) TO '${PASTA_SAIDA}${CLIENTE}\gruposempresariais.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';


-- EMPRESAS


COPY (
SELECT
	distinct on (empresa.cdempresa)
	regexp_replace(empresa.cdempresa::text, '[^0-9]', '', 'g')::integer CODIGO,
	replace(empresa.nmempresa,';','') DESCRICAO,
	CASE WHEN LENGTH(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g')) = 14 THEN REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g') ELSE NULL END CNPJ,
	CASE WHEN LENGTH(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g')) = 11 THEN REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g') ELSE NULL END CPF,
	replace(empresa.nmempresa,';','') RAZAOSOCIAL,
	NULL MASCARACONTA,
	NULL MASCARAGERENCIAL,
	NULL USADV,
	NULL FILANTROPICA,
	'M' TIPOPAGAMENTO,
	NULL TIPOCOOPERATIVA,
	NULL TIPOCONSTRUTORA,
	NULL NUMEROCERTIFICADO,
	NULL MINISTERIO,
	NULL DATAEMISSAOCERTIFICADO,
	NULL DATAVENCIMENTOCERTIFICADO,
	NULL NUMEROPROTOCOLORENOVACAO,
	NULL DATAPROTOCOLORENOVACAO,
	NULL DATAPUBLICACAODOU,
	NULL NUMEROPAGINADOU,
	NULL NOMECONTATO,
	NULL CPFCONTATO,
	RIGHT(empresa.nrtelefone,8) TELEFONEFIXOCONTATO,
	LEFT(empresa.nrtelefone,2) DDDTELFIXOCONTATO,
	NULL TELEFONECELULARCONTATO,
	NULL DDDTELCELULARCONTATO,
	NULL FAXCONTATO,
	NULL DDDFAXCONTATO,
	NULL EMAILCONTATO,
	NULL INATIVA,
	empresa.escritorio_data_entrada INICIOEXERCICIO,
	NULL INICIO_ATIVIDADES,
	NULL TRIBUTACAOPISCOFINS,
	NULL ALELONUMEROCONTRATO,
	NULL TIPOPONTOELETRONICO,
	NULL MULTIPLASTABELASRUBRICA,
	NULL NUMEROSIAFI,
	NULL ACORDOINTERNACIONALISENCAOMULTA,
	NULL TIPOSITUACAOPJ,
	NULL TIPOSITUACAOPF,
	NULL REGIMEPROPRIOPREVIDENCIASOCIAL,
	NULL DESCRICAOLEISEGURADODIFERENCIADO,
	NULL VALORSUBTETOEXECUTIVO,
	NULL VALORSUBTETOLEGISLATIVO,
	NULL VALORSUBTETOJUDICIARIO,
	NULL VALORSUBTETOTODOSPODERES,
	NULL ANOSMAIORIDADEDEPENDENTEEXECUTIVO,
	NULL ANOSMAIORIDADEDEPENDENTELEGISLATIVO,
	NULL ANOSMAIORIDADEDEPENDENTEJUDICIARIO,
	NULL ANOSMAIORIDADEDEPENDENTETODOSPODERES,
	regexp_replace(empresa.obs, '[\n\r]+', ' - ', 'g' ) OBSERVACAO,
	NULL USAPONTOWEB,
	NULL ENTIDADEEDUCATIVA,
	NULL EMPRESADETRABALHOTEMPORARIO,
	NULL ENTEFEDERATIVO,
	NULL CNPJENTEFEDERATIVO,
	NULL SUBTETOENTEFEDERATIVO,
	NULL VALORSUBTETOENTEFEDERATIVO,
	NULL NUMEROREGISTROTRABALHOTEMPORARIOMTE,
	regexp_replace(emp.cdempresa::text, '[^0-9]', '', 'g')::integer GRUPOEMPRESARIAL,
	empresa.cdnaturezajuridica NATUREZAJURIDICA,
	case when empresa.cdcnae in ('5212400','0951600','3999') then NULL else empresa.cdcnae end CNAE,
	NULL MOEDA,
	NULL MUNICIPIOENTEFEDERATIVO,
	NULL CAEPF,
	replace(empresa.nmcidade,';','') CIDADE,
	empresa.nrinscrestadual INSCRICAOESTADUAL,
	empresa.nrinscrmunicipal INSCRICAOMUNICIPAL,
	replace(empresa.nmfantasia,';','') NOMEFANTASIA,
	replace(empresa.nmemail,';','') EMAIL,
	NULL SITE,
	replace(empresa.dsendereco,';','') LOGRADOURO,
	NULL NUMERO,
	replace(empresa.endereco_complemento,';','') COMPLEMENTO,
	replace(empresa.nmbairro,';','') BAIRRO,
	REGEXP_REPLACE(empresa.nrcep,'[^0-9]','','g') CEP,
	NULL TIPOSIMPLES,
	LEFT(empresa.nrtelefone,2) DDDTEL,
	RIGHT(empresa.nrtelefone,8) TELEFONE,
	NULL DDDFAX,
	NULL FAX,
	NULL BLOQUEADO,
	NULL SELECIONARCFOP,
	empresa.dsatividade RAMOATIVIDADE,
	NULL ANOFISCAL,
	NULL FINAL_ATIVIDADES,
	NULL DATAREGISTRO,
	NULL SUFRAMA,
	NULL REGISTRO,
	NULL REPRESENTANTE,
	NULL CPFREPRESENTANTE,
	NULL DDDTELREPRESENTANTE,
	NULL TELEFONEREPRESENTANTE,
	NULL RAMALREPRESENTANTE,
	NULL DDDFAXREPRESENTANTE,
	NULL FAXREPRESENTANTE,
	NULL EMAILREPRESENTANTE,
	NULL CAIXAPOSTAL,
	NULL UFCAIXAPOSTAL,
	NULL CEPCAIXAPOSTAL,
	NULL ACIDENTETRABALHO,
	NULL NUMEROPROPRIETARIOS,
	NULL NUMEROFAMILIARES,
	NULL NUMEROCONTA,
	NULL CODIGOTERCEIROS,
	NULL PORTE,
	NULL FAZPARTEPAT,
	NULL ALIQUOTAFILANTROPICA,
	empresa.vlcapital CAPITALSOCIAL,
	NULL PAGAPIS,
	NULL TIPOCONTA,
	NULL CEI,
	NULL DATANASCIMENTOREPRESENTANTE,
	NULL SEXOREPRESENTANTE,
	NULL CONTACORRENTEPAGADORA,
	NULL IDENTIFICACAOREGISTRO,
	NULL CONTRIBUINTEIPI,
	NULL TIPOCONTROLEPONTO,
	NULL CENTRALIZACONTRIBUICAOSINDICAL,
	NULL EXCESSOSUBLIMITE,
	NULL ALIQUOTAAPLICAVEL,
	NULL ALELOCODIGOPESSOAJURIDICA,
	NULL NISREPRESENTANTE,
	NULL DATAIMPLANTACAOSALDO,
	NULL CONTRIBUINTEICMS,
	NULL CENTRALIZADORSEFIP,
	NULL TIPOCAEPF,
	regexp_replace(emp.cdempresa::text, '[^0-9]', '', 'g')::integer EMPRESA,
	NULL TIPOLOGRADOURO,
	NULL BANCO,
	NULL AGENCIA,
	NULL CONTADOR,
	NULL SINDICATO,
	fpas.cdfpas FPAS,
	codmunicipio IBGE,
	NULL ID_CENTRO_CUSTO
FROM wphd.empresa
INNER JOIN wphd.empresa emp ON (
					LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8)
			       )
			       OR
			       (
					LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g')
			       )
LEFT JOIN wphd.municipiosibge ON empresa.cduf = municipiosibge.sguf AND empresa.idmunicipio = municipiosibge.idmunicipio
LEFT JOIN wdp.empdp ON empresa.cdempresa = empdp.idempresa::INTEGER
LEFT JOIN wdp.fpas ON empdp.cdfpasgrps = fpas.idfpas
WHERE emp.cdempresa not in ('99997', '99998', '99999') 
) TO '${PASTA_SAIDA}${CLIENTE}\empresas.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';

-- CARGOS

UPDATE wdp.funcoesb SET nrcbonovo = '516110' WHERE nrcbonovo = '516010';
UPDATE wdp.funcoesb SET nrcbonovo = '234520' WHERE nrcbonovo = '023120';
UPDATE wdp.funcoesb SET nrcbonovo = '514320' WHERE nrcbonovo = '505142';
UPDATE wdp.funcoesb SET nrcbonovo = '131310' WHERE nrcbonovo = '501313';
UPDATE wdp.funcoesb SET nrcbonovo = '516110' WHERE nrcbonovo IN ('30120','24220','93950','55290','03890','53390','19790','85190','58330','85510','33990','39130','33190','39330','92990','85858','92290','33115','03805','89990','84390','30220','08320','39390','45190','72520','79190','79120','514210','45490','111111','73210','32390','433115','77645','31190','79490','08490','30110','42150','512215');

--CREATE TEMP TABLE cargos (codigo VARCHAR, novocodigo BIGSERIAL);
drop table if exists cargos;
CREATE TABLE cargos (codigo VARCHAR, novocodigo BIGSERIAL);

INSERT INTO cargos (codigo)
SELECT cdchamada
FROM wdp.funcoesb
ORDER BY cdchamada;

COPY (
SELECT distinct
	RIGHT('000' || cargos.novocodigo::VARCHAR,4) codigo,
	COALESCE(funcoesb.nmfuncao,'CARGO SEM NOME') descricao,
	COALESCE(funcoesb.nmfuncao,'CARGO SEM NOME') nome,
	NULL experiencia,
	NULL maiorsalmercado,
	NULL menorsalmercado,
	NULL requisitos,
	NULL diasexperienciacontrato,
	NULL diasprorrogacaocontrato,
	CASE
	WHEN funcoesb.nrcbonovo='' THEN '5121'
	WHEN funcoesb.nrcbonovo IS NULL THEN '5121'
            ELSE funcoesb.nrcbonovo
	    END cbo,
	NULL estabelecimento,
	NULL grauinstrucao,
	NULL departamento,
	NULL horario,
	NULL lotacao,
	NULL sindicato,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial,
	null pontuacao,
	null pisominimo,
	null cargopublico,
	null acumulacaocargos,
	null contagemespecial,
	null dedicacaoexclusiva,
	null numeroleicargo,
	null dataleicargo,
	null situacaoleicargo,
	null desabilitado
FROM wphd.empresa, wphd.empresa emp, wdp.funcoesb
INNER JOIN cargos ON funcoesb.cdchamada = cargos.codigo
WHERE (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8))
                                                              OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))
) TO '${PASTA_SAIDA}${CLIENTE}\cargos.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';



-- SINDICATO

COPY (
SELECT
	cdchamada codigo,
	LEFT(nmsindicato,60) descricao,
	CASE
		WHEN nrcgc='' OR nrcgc IS NULL THEN '99999999999999' ELSE REGEXP_REPLACE(nrcgc,'[^0-9]','','g')
	END cnpj,
	NULL calculoeacumulativo,
	NULL calculanofim,
	NULL patronal,
	NULL somentemaioranuenio,
	NULL multafgts,
	NULL mesesmediamaternidade,
	NULL diadissidio,
	NULL diasaviso,
	NULL mediaferiaspelomaiorvalor,
	NULL media13pelomaiorvalor,
	NULL mediarescisaopelomaiorvalor,
	NULL mesdesconto,
	NULL mesdissidio,
	nrmedia::TEXT mesesmediaferias,
	nrmedia::TEXT mesesmediarescisao,
	nrmedia::TEXT mesesmedia13,
	NULL numeradorfracao,
	NULL denominadorfracao,
	nmendereco logradouro,
	nrendereco numero,
	nmcomplemento complemento,
	nmbairro bairro,
	nmcidade cidade,
	case when REGEXP_REPLACE(nrcep,'[^0-9]','','g')=''
	then null
	else REGEXP_REPLACE(nrcep,'[^0-9]','','g')
	end as cep,
	NULL codigocontribuicao,
	cdentidade codigoentidadesindical,
	vlpiso::TEXT pisosalarial,
	cduf estado,
	NULL contato,
	case when REGEXP_REPLACE(nrtelefone,'[^0-9]','','g')=''
	then null
	else REGEXP_REPLACE(nrtelefone,'[^0-9]','','g')
	end as telefone,
	CASE WHEN nrtelefone='' then null
	when nrtelefone is null then null
	else right(nrddd,2) 
	END as dddtel,
	NULL fax,
	NULL dddfax,
	NULL email,
	NULL mesassistencial,
	NULL fpas,
	NULL codigoterceiros
FROM wdp.sind
UNION
SELECT DISTINCT
	'PADRAO' codigo,
	'SINDICATO PADRAO' descricao,
	'99999999999999' cnpj,
	NULL calculoeacumulativo,
	NULL calculanofim,
	NULL patronal,
	NULL somentemaioranuenio,
	NULL multafgts,
	NULL mesesmediamaternidade,
	NULL diadissidio,
	NULL diasaviso,
	NULL mediaferiaspelomaiorvalor,
	NULL media13pelomaiorvalor,
	NULL mediarescisaopelomaiorvalor,
	NULL mesdesconto,
	NULL mesdissidio,
	NULL mesesmediaferias,
	NULL mesesmediarescisao,
	NULL mesesmedia13,
	NULL numeradorfracao,
	NULL denominadorfracao,
	NULL logradouro,
	NULL numero,
	NULL complemento,
	NULL bairro,
	NULL cidade,
	NULL cep,
	NULL codigocontribuicao,
	NULL codigoentidadesindical,
	NULL pisosalarial,
	NULL estado,
	NULL contato,
	NULL telefone,
	NULL dddtel,
	NULL fax,
	NULL dddfax,
	NULL email,
	NULL mesassistencial,
	NULL fpas,
	NULL codigoterceiros
FROM wdp.sind
) TO '${PASTA_SAIDA}${CLIENTE}\sindicatos.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';



-- LOTACOES

COPY (
SELECT
	empresa.cdempresa::VARCHAR codigo,
	empresa.nmempresa descricao,
	NULL enderecodiferente,
	NULL outrasentidadesdiferente,
	NULL agentenocivo,
	NULL nuncaexpostoagentenocivo,
	NULL tipologradouro,
	NULL logradouro,
	NULL numero,
	NULL complemento,
	NULL bairro,
	NULL cidade,
	NULL cep,
	NULL uf,
	NULL municipio,
	NULL fpas,
	NULL codigoterceiros,
	NULL aliquotaterceiros,
	empresa.cdempresa estabelecimento,
	NULL processo,
	NULL tomador,
	NULL obra,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial,
	NULL tipolotacaoesocial
FROM wphd.empresa
INNER JOIN wphd.empresa emp ON (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8))
			    OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))

UNION ALL

SELECT
	empresa.cdempresa || depto.cdchamada  codigo,
	depto.nmdepartamento descricao,
	'S' enderecodiferente,
	NULL outrasentidadesdiferente,
	NULL agentenocivo,
	NULL nuncaexpostoagentenocivo,
	depto.tipo_logradouro tipologradouro,
	depto.nmendereco logradouro,
	CASE WHEN depto.nrendereco = '' THEN NULL ELSE depto.nrendereco END numero,
	CASE WHEN depto.nmcomplemento = '' THEN NULL ELSE depto.nmcomplemento END complemento,
	depto.nmbairro bairro,
	depto.nmcidade cidade,
	REPLACE(depto.nrcep,'-','') cep,
	depto.cduf uf,
	depto.cdmunicipiorais municipio,
	NULL fpas,
	NULL codigoterceiros,
	NULL aliquotaterceiros, 
	empresa.cdempresa estabelecimento,
	NULL processo,
	NULL tomador,
	NULL obra,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial,
	NULL tipolotacaoesocial
FROM wdp.depto
INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa 
INNER JOIN wphd.empresa emp ON (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8))
			    OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))
				
) TO '${PASTA_SAIDA}${CLIENTE}\lotacoes.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';

-- DEPARTAMENTO

COPY (
SELECT
	'PADRAO' codigo,
	'PADRAO' descricao,
	empresa.cdempresa estabelecimento,
	case when emp.cdempresa is null then empresa.cdempresa
	else emp.cdempresa end as empresa,
	case when emp.cdempresa is null then empresa.cdempresa
	else emp.cdempresa end as  grupoempresarial,
	null as nomeresponsavel
FROM wphd.empresa
left JOIN wphd.empresa emp ON (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8))
			    OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))

UNION ALL

SELECT
	depto.cdchamada codigo,
	left(depto.nmdepartamento,40) descricao,
	empresa.cdempresa estabelecimento,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial,
	null as nomeresponsavel
FROM wdp.depto
INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa
left JOIN wphd.empresa emp ON (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8))
			    OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(depto.nrcgc,'[^0-9]','','g'))
			
) TO '${PASTA_SAIDA}${CLIENTE}\departamentos.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';


--NIVEIS DE CARGOS

COPY (
SELECT DISTINCT
	funcoesb.cdchamada codigo,
	COALESCE(funcoesb.nmfuncao,'CARGO SEM NOME') descricao,
	NULL valorsalario,
	NULL valoranterior,
	NULL "data",
	NULL dataatualizacaoanterior,
	RIGHT('000' || cargos.novocodigo::VARCHAR,4) cargo,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial
FROM wphd.empresa, wphd.empresa emp, wdp.funcoesb 
INNER JOIN cargos ON funcoesb.cdchamada = cargos.codigo
WHERE (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8))
                                                              OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))
) TO '${PASTA_SAIDA}${CLIENTE}\niveiscargos.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';

-- JORNADAS

COPY (
with jornadas as (
SELECT distinct
	horario.cdchamada codigo,
	horario.dshorario descricao,
	LEFT(horario.hrentrada,2) || ':' || RIGHT(horario.hrentrada,2) entrada,
	LEFT(horario.hrsaida,2) || ':' || RIGHT(horario.hrsaida,2) saida,
	NULL duracaointervalo,
	NULL descricaotipojornada,
	NULL flexivel,
	case when horario.hrsaidaalmoco is null then null when replace(horario.hrsaidaalmoco,' ','') = '' then null else LEFT(horario.hrsaidaalmoco,2) || ':' || RIGHT(horario.hrsaidaalmoco,2) end intervalo_inicio_01,
	case when 
	horario.hrvoltaalmoco is null then null when replace(horario.hrvoltaalmoco,' ','') = '' then null
	else LEFT(horario.hrvoltaalmoco,2) || ':' || RIGHT(horario.hrvoltaalmoco,2) end intervalo_fim_01,
	NULL intervalo_inicio_02,
	NULL intervalo_fim_02,
	NULL intervalo_inicio_03,
	NULL intervalo_fim_03,
	NULL intervalo_inicio_04,
	NULL intervalo_fim_04,
	NULL intervalo_inicio_05,
	NULL intervalo_fim_05,
	NULL intervalo_inicio_06,
	NULL intervalo_fim_06,
	NULL intervalo_inicio_07,
	NULL intervalo_fim_07,
	NULL intervalo_inicio_08,
	NULL intervalo_fim_08,
	NULL intervalo_inicio_09,
	NULL intervalo_fim_09,
	NULL intervalo_inicio_10,
	NULL intervalo_fim_10,
	emp.cdempresa empresa,
	emp.cdempresa grupoempresarial
FROM wdp.horario, wphd.empresa, wphd.empresa emp
WHERE (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8))
                                                              OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))
	
	)
	
	select codigo,
replace(descricao,';',':'),
entrada,
saida,
duracaointervalo,
descricaotipojornada,
flexivel,
case when INTERVALO_INICIO_01 <= entrada then null
when intervalo_inicio_01 > saida then null
else intervalo_inicio_01
end as intervalo_inicio_01,
case when intervalo_inicio_01 is null then null
when INTERVALO_INICIO_01 <= entrada then null
when intervalo_inicio_01 > saida then null
else intervalo_fim_01
end as INTERVALO_FIM_01,
intervalo_inicio_02,
intervalo_fim_02,
intervalo_inicio_03,
intervalo_fim_03,
intervalo_inicio_04,
intervalo_fim_04,
intervalo_inicio_05,
intervalo_fim_05,
intervalo_inicio_06,
intervalo_fim_06,
intervalo_inicio_07,
intervalo_fim_07,
intervalo_inicio_08,
intervalo_fim_08,
intervalo_inicio_09,
intervalo_fim_09,
intervalo_inicio_10,
intervalo_fim_10,
case when empresa is null or empresa::text='' then grupoempresarial
else empresa end as empresa,
grupoempresarial
FROM JORNADAS 
) TO '${PASTA_SAIDA}${CLIENTE}\jornadas.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';


-- HORARIOS

COPY (
SELECT
	horario.cdchamada codigo,
	replace(horario.dshorario,';','') descricao,
	NULL numerofolgasfixas,
	NULL diafolgaextra,
	NULL diasemanafolgaextra,
	NULL horarioesperadoemfolgas,
	NULL atrasosegunda,
	NULL atrasoterca,
	NULL atrasoquarta,
	NULL atrasoquinta,
	NULL atrasosexta,
	NULL atrasosabado,
	NULL atrasodomingo,
	NULL dsrsobredomingoseferiados,
	horario.cdchamada jornadasegunda,
	NULL jornadaterca,
	NULL jornadaquarta,
	NULL jornadaquinta,
	NULL jornadasexta,
	NULL jornadasabado,
	NULL jornadadomingo,
	NULL jornadaoutros,
	regexp_replace(empresa.cdempresa::text, '[^0-9]', '', 'g')::integer empresa,
	regexp_replace(empresa.cdempresa::text, '[^0-9]', '', 'g')::integer grupoempresarial,
	NULL DESCRICAOESCALA,
	NULL DESCONSIDERARDSRFOLGASFIXAS,
	NULL DESABILITADO
FROM wdp.horario, wphd.empresa, wphd.empresa emp
WHERE (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 14 AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),6),4) = '0001' AND LEFT(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g'),8) =  LEFT(REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'),8))
                                                              OR (LENGTH(REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g')) = 11 AND REGEXP_REPLACE(emp.nrcgc,'[^0-9]','','g') = REGEXP_REPLACE(empresa.nrcgc,'[^0-9]','','g'))
) TO '${PASTA_SAIDA}${CLIENTE}\horarios.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';
-- BANCOS

COPY (
SELECT
	cdchamada codigo,
	nmbanco descricao,
	cdchamada numero,
	NULL tipoimpressao,
	NULL codigoispb
FROM wdp.bancos
) TO '${PASTA_SAIDA}${CLIENTE}\bancos.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';


-- AGENCIAS


COPY (
SELECT
	left(agencia.cdchamada,7) codigo,
    CASE 
        WHEN agencia.nmagencia = '' AND agencia.cdchamada = '1043' THEN 'DESCRICAO SEM NOME'
        WHEN agencia.nmagencia IS NULL AND agencia.cdchamada = '1043' THEN 'DESCRICAO SEM NOME'
        ELSE left(REPLACE(agencia.nmagencia, CHR(150), '-'), 20)
    END descricao,
	CASE 
        WHEN SUBSTRING(agencia.cdchamada, LENGTH(agencia.cdchamada) - 1, 1) = '-' THEN SUBSTRING(agencia.cdchamada, 1, LENGTH(agencia.cdchamada) - 2) 
        ELSE agencia.cdchamada 
    END agencianumero,
	CASE 
        WHEN SUBSTRING(agencia.cdchamada, LENGTH(agencia.cdchamada) - 1, 1) = '-' THEN RIGHT(agencia.cdchamada, 1) 
        ELSE NULL 
    END digitoverificador,
	NULL logradouro,
	NULL numero,
	NULL complemento,
	NULL bairro,
	NULL cidade,
	NULL estado,
	NULL cep,
	NULL contato,
	NULL telefone,
	NULL dddtel,
	bancos.cdchamada banco
FROM wdp.agencia
INNER JOIN wdp.bancos ON agencia.idbanco = bancos.idbanco
UNION ALL
SELECT DISTINCT
	'9999' codigo,
   'FUNC. SEM AGENCIA' descricao,
	'9999' agencianumero,
	NULL digitoverificador,
	NULL logradouro,
	NULL numero,
	NULL complemento,
	NULL bairro,
	NULL cidade,
	NULL estado,
	NULL cep,
	NULL contato,
	NULL telefone,
	NULL dddtel,
	'033' banco
FROM wdp.agencia


) TO '${PASTA_SAIDA}${CLIENTE}\agencias.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';




-- TARIFASCONCESSIONARIASVTS

COPY (
	SELECT DISTINCT ON (passagb.cdchamada, passagb.nmpassagem)
	passagb.cdchamada CODIGO,
	left(passagb.nmpassagem,30) DESCRICAO,
	'001' CODIGOCONCESSIONARIAVT,
	passagv.vlpreco VALOR,
	passagb.tpconducao TIPO,
	NULL CODIGOEXTERNO,
	NULL REDERECARGA
	FROM wdp.passagb
	INNER JOIN wdp.passagv ON passagb.idpassagem = passagv.idpassagem
	ORDER BY passagb.cdchamada, passagb.nmpassagem, passagv.dtinicial DESC
) TO '${PASTA_SAIDA}${CLIENTE}\tarifasconcessionariasvts.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';


-- TARIFASCONCESSIONARIASVTSTRABALHADORES
DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	CREATE TEMP TABLE tarifas(CODIGO varchar, DESCRICAO varchar, FUNCIONARIO varchar, CONCESSIONARIAVT varchar, TARIFAVT varchar, QUANTIDADE varchar, EMPRESA varchar, GRUPOEMPRESARIAL varchar, dtfinal varchar);

	sqlcmd := 'INSERT INTO tarifas ';

	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'p_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
			
		 )
	LOOP

		IF (sqlcmd <> 'INSERT INTO tarifas ') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT distinct on (empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6), passagb.cdchamada)
					NULL CODIGO,
					NULL DESCRICAO,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) FUNCIONARIO,
					''001'' CONCESSIONARIAVT,
					passagb.cdchamada TARIFAVT,
					frmpasq.qtdmensal QUANTIDADE,
					emp.cdempresa EMPRESA,
					emp.cdempresa GRUPOEMPRESARIAL,
					frmpasq.dtfinal
					FROM wphd.empresa,wphd.empresa emp, wdp.frmpasq
					INNER JOIN wdp.passagb ON frmpasq.idpassagem = passagb.idpassagem
					INNER JOIN wdp.p' || r.tablename || ' AS passagem ON frmpasq.idformacaopassagem = passagem.idformacaopassagens
					INNER JOIN wdp.f' || r.tablename || ' AS funcionario ON passagem.idfuncionario = funcionario.idfuncionario
					WHERE
					(
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
							AND
							LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
							AND
							LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
							)
						OR
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
							AND
							REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
						)
					)
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
					';
	END LOOP;
	
	EXECUTE sqlcmd;
	
	COPY (SELECT DISTINCT ON (FUNCIONARIO, TARIFAVT) CODIGO,DESCRICAO,FUNCIONARIO,CONCESSIONARIAVT,TARIFAVT,QUANTIDADE,EMPRESA,GRUPOEMPRESARIAL FROM tarifas ORDER BY FUNCIONARIO, TARIFAVT, dtfinal desc) TO '${PASTA_SAIDA}${CLIENTE}\tarifasconcessionariasvtstrabalhadores.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'WIN1252';
	
	DROP TABLE tarifas;
END
$$;
-- TRABALHADORES

DO
$$
DECLARE 
    r RECORD;
    sqlcmd TEXT;
    table_with_data BOOLEAN;
BEGIN
    -- Inicializa o comando SQL
    sqlcmd := 'COPY (';

    -- Faz loop pelas tabelas
    FOR r IN (
        SELECT
            RIGHT(t.tablename, 5) AS tablename,
            t.tablename AS full_table_name
        FROM pg_tables t
        JOIN wphd.empresa e
            ON RIGHT(t.tablename, 5) = LPAD(CAST(e.cdempresa AS TEXT), 5, '0')
        WHERE t.schemaname = 'wdp'
        AND t.tablename LIKE 'f_____'  -- Tabelas que começam com 'f'
        AND RIGHT(t.tablename, 1) BETWEEN '0' AND '9'
    )
    LOOP
        -- Verifica dinamicamente se a tabela tem dados
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM wdp.%I LIMIT 1)', r.full_table_name) INTO table_with_data;

        IF table_with_data THEN
            -- Concatena o comando para cada tabela que tenha dados
            IF (sqlcmd <> 'COPY (') THEN
                sqlcmd := sqlcmd || ' UNION ';
            END IF;
				sqlcmd:=sqlcmd || 'SELECT
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) codigo,
					funcionario.nmfuncionario descricao,
					case when funcionario.dtadmissao::DATE is null then ''01/01/1900''
					else funcionario.dtadmissao::DATE
					end as dataadmissao,
					funcionario.tipo_admissao tipoadmissao,
					NULL primeiroemprego,
					NULL tiporegimetrabalhista,
					funcionario.regime_previdenciario tiporegimeinss,
					NULL tipoatividade,
					CASE WHEN funcionario.vlsalariobase IS NULL OR funcionario.vlsalariobase <= 0 THEN 0.01 ELSE coalesce(funcionario.vlsalariobase,''0'') END salariofixo,
					NULL salariovariavel,
					funcionario.tpformapgto unidadesalariofixo,
					NULL unidadesalariovariavel,
					CASE funcionario.strestantecontrato WHEN ''0'' THEN NULL WHEN NULL THEN NULL ELSE funcionario.dtadmissao::DATE+funcionario.strestantecontrato::INTEGER END datafimcontrato,
					(funcionario.dtvctoexperiencia::DATE-funcionario.dtadmissao::DATE)+1 diasexperienciacontrato,
					(funcionario.dtprorrogacaoexperiencia::DATE-funcionario.dtvctoexperiencia::DATE) diasprorrogacaocontrato,
					NULL numerohorasmensais,
					NULL numerodiasperiodo,
					CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
						WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
						WHEN '''' THEN NULL
						ELSE nrcontacorrente
					END numerocontasalario,
					CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
						WHEN ''-'' THEN RIGHT(funcionario.nrcontacorrente,1)
						ELSE NULL
					END numerocontasalariodv,
					funcionario.nrcontafgts numerocontafgts,
					CASE WHEN funcionario.tipo_conta = 1 AND nrcontacorrente = '''' THEN ''D''
					     WHEN funcionario.tipo_conta = 1 THEN ''CC''
					     WHEN funcionario.tipo_conta = 2 THEN ''CP''
					     WHEN funcionario.tipo_conta = 3 THEN ''CS''
					     ELSE ''D''
					END tiporecebimentosalario,
					funcionario.dtopcaofgts::DATE dataopcaofgts,
					NULL numeroreciborescisao,
					funcionario.dtdemissao::DATE datarescisao,
					funcionario.tpreintegracao tiporeintegracao,
					NULL numeroleianistia,
					NULL datareintegracaoretroativa,
					NULL datareintegracaoretorno,
					funcionario.tpsexo sexo,
					CASE funcionario.tpestadocivil
						WHEN ''C'' THEN ''C''
						WHEN ''U'' THEN ''U''
						WHEN ''S'' THEN ''S''
						ELSE NULL
					END estadocivil,
					CASE WHEN funcionario.dtnascimento IS NULL THEN ''01/01/1999''
					ELSE funcionario.dtnascimento::VARCHAR
					END datanascimento,
					funcionario.cdufnascimento ufnascimento,
					funcionario.nmmunnascimento cidadenascimento,
					funcionario.nmpai nomepai,
					funcionario.nmmae nomemae,
					funcionario.data_chegada datachegadapais,
					NULL datanaturalizacao,
					funcionario.casado_com_brasileiro casadocombrasileiro,
					funcionario.filho_com_brasileiro filhobrasileiro,
					CASE funcionario.deficiencia_visual WHEN TRUE THEN ''S'' ELSE NULL END deficientevisual,
					CASE funcionario.deficiencia_auditiva WHEN TRUE THEN ''S'' ELSE NULL END deficienteauditivo,
					funcionario.streabilitado reabilitado,
					funcionario.nrctps numeroctps,
					funcionario.nrseriectps seriectps,
					funcionario.cdufctps ufctps,
					funcionario.dtexpedicaoctps::DATE dataexpedicaoctps,
					NULL numeroric,
					NULL orgaoemissorric,
					NULL dataexpedicaoric,
					NULL ufric,
					NULL cidaderic,
					REGEXP_REPLACE(funcionario.nridentidade,''[^0-9]'','''',''g'') numerorg,
					funcionario.nmorgaoexpedidor orgaoemissorrg,
					funcionario.dtexpedicao::DATE dataexpedicaorg,
					funcionario.cdufexpedicao ufrg,
					funcionario.inscricao_orgao_classe numerooc,
					funcionario.emissor_orgao_classe orgaoemissoroc,
					funcionario.data_expedicao_orgao_classe dataexpedicaooc,
					funcionario.data_validade_orgao_classe datavalidadeoc,
					funcionario.nrcarteirahabilitacao numerocnh,
					funcionario.orgao_emissor_cnh orgaoemissorcnh,
					funcionario.data_expedicao_cnh::DATE dataexpedicaocnh,
					funcionario.dtvctohabilitacao::DATE datavalidadecnh,
					funcionario.data_primeira_cnh::DATE dataprimeirahabilitacaocnh,
					funcionario.tphabilitacao categoriacnh,
					NULL numeropassaporte,
					NULL orgaoemissorpassaporte,
					NULL ufpassaporte,
					NULL dataexpedicaopassaporte,
					NULL datavalidadepassaporte,
					funcionario.nrcpf cpf,
					funcionario.nrpispasep nis,
					funcionario.nrdecretonaturalizado numeronaturalizacao,
					funcionario.nrtituloeleitor numerote,
					case when funcionario.nrzonaeleitoral = '''' then NULL
                                             when funcionario.nrzonaeleitoral = '','' then NULL
                                             else funcionario.nrzonaeleitoral
                                             end zonate,
					case when funcionario.nrsecaoeleitoral = '''' then NULL
                                             when funcionario.nrsecaoeleitoral = '','' then NULL
                                             else LTRIM(RTRIM(funcionario.nrsecaoeleitoral,''''''''),'' '')
                                             end secaote,
					NULL ufte,
					NULL numeroatestadoobito,
					CASE
						WHEN funcionario.dtvctoatestado IS NOT NULL THEN funcionario.dtvctoatestado::DATE
						WHEN funcionario.dtdemissao IS NOT NULL THEN funcionario.dtdemissao::DATE
						WHEN (EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || EXTRACT(MONTH FROM funcionario.dtadmissao) || ''-'' || EXTRACT(DAY FROM funcionario.dtadmissao))::DATE <= now()::DATE AND EXTRACT(MONTH FROM funcionario.dtadmissao) = 2 AND EXTRACT(DAY FROM funcionario.dtadmissao) = 29 THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-'' || ''02-28'')::DATE
						WHEN (EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || EXTRACT(MONTH FROM funcionario.dtadmissao) || ''-'' || EXTRACT(DAY FROM funcionario.dtadmissao))::DATE <= now()::DATE THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-'' || RIGHT(''0'' || EXTRACT(MONTH FROM funcionario.dtadmissao)::VARCHAR(2),2) || ''-'' || RIGHT(''0'' || EXTRACT(DAY FROM funcionario.dtadmissao)::VARCHAR(2),2))::DATE
						WHEN EXTRACT(MONTH FROM funcionario.dtadmissao) = 2 AND EXTRACT(DAY FROM funcionario.dtadmissao) = 29 THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-02-28'')::DATE
						ELSE coalesce(coalesce((EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || RIGHT(''0'' || EXTRACT(MONTH FROM funcionario.dtadmissao)::VARCHAR(2),2) || ''-'' || RIGHT(''0'' || EXTRACT(DAY FROM funcionario.dtadmissao)::VARCHAR(2),2))::DATE,funcionario.dtadmissao+ interval ''365 days''),''01/01/1901'')
					END datavencimentoatestadomedico,
					CASE funcionario.tipo_certidao_civil
						WHEN 1 THEN ''N''
						WHEN 2 THEN ''C''
						ELSE NULL
					END tipocertidao,
					RIGHT(funcionario.termo_matricula_certidao_civil,30) numerocertidao,
					funcionario.livro_certidao_civil livrocertidao,
					right(funcionario.folha_certidao_civil, 5) folhacertidao,
					funcionario.data_certidao_civil dataexpedicaocertidao,
					funcionario.municipio_certidao_civil cidadecertidao,
					funcionario.uf_certidao_civil ufcertidao,
					funcionario.cartorio_certidao_civil cartoriocertidao,
					left(funcionario.nrcertificadoreservista, 15) numerocr,
					NULL dataexpedicaocr,
					RIGHT(funcionario.nrserie,5) seriecr,
					replace(funcionario.nmendereco, '';'', '''')  logradouro,
					funcionario.nrendereco numero,
					funcionario.nmcomplemento complemento,
					funcionario.nmbairro bairro,
					funcionario.nmcidade cidade,
					REPLACE(funcionario.nrcep,''-'','''') cep,
					funcionario.nrtelefone telefone,
					NULL email,
					funcionario.quantidade_dias_ferias saldoferias,
					coalesce(COALESCE(funcferias.dtperiodoaquisinicial,funcionario.dtadmissao),''01/01/1900'')::DATE inicioperiodoaquisitivoferias,
					NULL dataproximasferias,
					CASE WHEN funcionario.vlsaldofgts <= 0 THEN NULL ELSE funcionario.vlsaldofgts END saldofgts,
					CASE WHEN funcionario.stadiantamento = ''S'' THEN funcionario.aladiantamento ELSE 0 END percentualadiantamento,
					''S'' descontainss,
					NULL tinhaempregonoaviso,
					funcionario.tpcontribuicaosindical sindicalizado,
					NULL datainicioanuenio,
					NULL jornadareduzida,
					funcionario.stavisoindenizado teveavisoindenizado,
					NULL fgtsmesanteriorrescisaorecolhi,
					funcionario.nrcartaoticket numerocartaovt,
					NULL diasemanacommeiovt,
					regexp_replace(funcionario.dsobservacao, ''[\n\r]+'', '''', ''g'' ) observacao,
					NULL cnpjempresaanterior,
					NULL matriculaempresaanterior,
					NULL dataadmissaoempresaanterior,
					NULL cnpjcendente,
					NULL matriculacedente,
					NULL dataadmissaocedente,
					NULL cnpjempresasucessora,
					NULL trabalhaemoutraempresa,
					RIGHT(funcionario.nrddd,2) dddtel,
					NULL sangue,
					NULL ordemcalculoduplovinculo,
					CASE funcionario.deficiencia_intelectual WHEN TRUE THEN ''S'' ELSE NULL END deficienteintelectual,
					CASE funcionario.deficiencia_mental WHEN TRUE THEN ''S'' ELSE NULL END deficientemental,
					CASE funcionario.tipo_admissao WHEN 2 THEN ''F'' ELSE ''N'' END motivoadmissao,
					NULL quantidademediahorassemanais,
					NULL motivocontratacaotemporaria,
					NULL avisoindenizadopago,
					funcionario.dtavisoprevio::DATE dataprojecaoavisopreviopago,
					NULL recolheufgtsmesanterior,
					funcionario.tipo_jornada_trabalho regimedejornada,
					NULL mesestrabalhadosoutrasempresas,
					CASE funcionario.deficiencia_fisica WHEN TRUE THEN ''S'' ELSE NULL END deficientefisico,
					NULL jornadacumpridasemanademissao,
					CASE funcionario.rescisao_sabado_compensado WHEN TRUE THEN ''S'' WHEN FALSE THEN ''N'' ELSE NULL END jornadasabadocompensadasemanad,
					funcionario.stprimtrabcaged cageddiariogerado,
					NULL nivelestagio,
					NULL areaatuacaoestagio,
					NULL apoliceseguroestagio,
					NULL nomesupervisorestagio,
					NULL cpfsupervisorestagio,
					NULL estagioobrigatorio,
					NULL percentualdeducaobaseirrf,
					NULL percentualdeducaobaseinss,
					NULL aliquotaiss,
					NULL motivodesligamentodiretor,
					funcionario.dttransferencia::DATE datatransferencia,
					NULL valorgratificacoesrescisao,
					NULL valorbancohorasrescisao,
					NULL mesesbancohorasrescisao,
					NULL mesesgratificacoesrescisao,
					NULL excluido,
					funcionario.nrcelular celular,
					NULL dddcel,
					null ufcnh,
					null matriculaesocial,
					empresa.cdempresa estabelecimento,
					funcionario.categoria_trabalhador_esocial categoriatrabalhador,
					NULL agentenocivo,
					CASE
						WHEN cargos.novocodigo=''0377'' THEN ''3547''
						ELSE COALESCE(funcoesb.nrcbonovo,''421205'') 
					END cbo,
					CASE
							WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN ''PADRAO''
							WHEN depto.cdchamada is not null then depto.cdchamada
							else ''PADRAO''
						END  departamento,
					CASE tpinstrucao	
						WHEN ''1'' THEN ''01''
						WHEN ''2'' THEN ''02''
						WHEN ''3'' THEN ''03''
						WHEN ''4'' THEN ''04''
						WHEN ''5'' THEN ''05''
						WHEN ''6'' THEN ''06''
						WHEN ''7'' THEN ''07''
						WHEN ''8'' THEN ''08''
						WHEN ''9'' THEN ''09''
						WHEN ''10'' THEN ''10''
						ELSE ''07''
					END grauinstrucao,
					CASE
						WHEN horario.cdchamada IS NULL OR horario.cdchamada = '''' then ''00001''
						ELSE horario.cdchamada 
					END horario,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒÆ’Ã†â€™O PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒÆ’Ã†â€™O PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					COALESCE(RIGHT(''000'' || cargos.novocodigo::VARCHAR,4),''0001'') cargo,
					COALESCE(funcoesb.cdchamada,''000001'') nivelcargo,
					CASE
						WHEN sind.cdchamada IS NULL THEN ''PADRAO'' ::varchar
						ELSE sind.cdchamada
					END sindicato,
					CASE WHEN
							CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
								WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
								WHEN '''' THEN NULL
								ELSE nrcontacorrente
							END IS NULL THEN NULL
							ELSE COALESCE(agencia.cdchamada,''9999'')
					END agencia,
					CASE WHEN
							CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
								WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
								WHEN '''' THEN NULL
								ELSE nrcontacorrente
							END IS NULL THEN NULL
							ELSE COALESCE(bancos.cdchamada,''033'')
					END banco,
					NULL funcao,
					NULL municipionascimento,
					CASE
						WHEN funcionario.municipio_residencia_ibge_codigo = '''' THEN NULL
						ELSE funcionario.municipio_residencia_ibge_codigo 
					END municipioresidencia,
					CASE funcionario.pais_nascimento_id WHEN ''105'' THEN ''1058'' ELSE NULL END paisnascimento,
					CASE funcionario.pais_residencia_id WHEN ''105'' THEN ''1058'' ELSE NULL END paisresidencia,
					CASE funcionario.nmnacionalidade WHEN ''105'' THEN ''1058'' ELSE NULL END paisnacionalidade,
					NULL paisemissaopassaporte,
					CASE funcionario.tpcorraca
						WHEN ''0'' THEN ''I''
						WHEN ''2'' THEN ''B''
						WHEN ''4'' THEN ''N''
						WHEN ''6'' THEN ''A''
						WHEN ''8'' THEN ''P''
						ELSE NULL
					END raca,
					NULL simplesconcomitante,
					NULL tipofuncionario,
					CASE
						WHEN funcionario.tipo_logradouro = '''' THEN NULL
						ELSE funcionario.tipo_logradouro
					END tipologradouro,
					CASE funcionario.tpcausademissao
						WHEN ''11'' THEN ''02''
						WHEN ''12'' THEN ''06''
						WHEN ''21'' THEN ''07''
					END motivorescisao,
					NULL acordodeprorrogacao,
					NULL instituicaoensino,
					NULL instituicaointegradora,
					NULL processoadmissao,
					NULL processodemissao,
					NULL processoinss,
					NULL processoirrf,
					NULL processomenoraprendiz,
					NULL processoreintegracao,
					CASE funcionario.tpvinculo WHEN '''' THEN NULL ELSE funcionario.tpvinculo END vinculo,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa,wphd.empresa emp, wdp.f' || r.tablename || ' AS funcionario
				LEFT JOIN wdp.depto ON funcionario.iddepartamento = depto.iddepartamento
				LEFT JOIN wdp.funcoesb ON funcionario.idfuncao = funcoesb.idfuncao
				LEFT JOIN cargos ON funcoesb.cdchamada = cargos.codigo
				LEFT JOIN wdp.horario ON funcionario.idhorario = horario.idhorario
				LEFT JOIN wdp.sind ON funcionario.idsindicatogrcs = sind.idsindicato
				LEFT JOIN wdp.agencia ON funcionario.idagencia = agencia.idagencia AND funcionario.idbanco = agencia.idbanco
				LEFT JOIN wdp.bancos ON agencia.idbanco = bancos.idbanco
				LEFT JOIN (SELECT DISTINCT ON (idfuncionario) idfuncionario, dtperiodoaquisinicial::DATE FROM wdp.a' || r.tablename || ' ORDER BY idfuncionario, dtperiodoaquisinicial DESC) AS funcferias ON funcionario.idfuncionario = funcferias.idfuncionario
				WHERE
					(
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
						AND
							LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
						AND
							LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
						)
						OR
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
						AND
							REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
						)
					)
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
end if;
	END LOOP;
	
	-- AUTONOMOS
	
	FOR r IN (
			 SELECT
            RIGHT(t.tablename, 5) AS tablename,
            t.tablename AS full_table_name
        FROM pg_tables t
        JOIN wphd.empresa e
            ON RIGHT(t.tablename, 5) = LPAD(CAST(e.cdempresa AS TEXT), 5, '0')
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'tb_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9'
		 )
	 LOOP
        -- Verifica dinamicamente se a tabela tem dados
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM wdp.%I LIMIT 1)', r.full_table_name) INTO table_with_data;

        IF table_with_data THEN
            -- Concatena o comando para cada tabela que tenha dados
            IF (sqlcmd <> 'COPY (') THEN
                sqlcmd := sqlcmd || ' UNION ';
            END IF;
	
			sqlcmd:=sqlcmd || 'SELECT
						empresa.cdempresa || right(''000000000'' || funcionario.identificacontrib, 9) codigo,
						socios.nmsocio descricao,
						CASE WHEN funcionario.dtadmissao::DATE IS NULL THEN ''01/01/1900''
						ELSE funcionario.dtadmissao::DATE
						END AS dataadmissao,
						NULL tipoadmissao,
						NULL primeiroemprego,
						NULL tiporegimetrabalhista,
						NULL tiporegimeinss,
						NULL tipoatividade,
						coalesce(funcionario.vlsalario,''0'') salariofixo,
						NULL salariovariavel,
						funcionario.salario_tipo unidadesalariofixo,
						NULL unidadesalariovariavel,
						NULL datafimcontrato,
						NULL diasexperienciacontrato,
						NULL diasprorrogacaocontrato,
						NULL numerohorasmensais,
						NULL numerodiasperiodo,
						CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
							WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
							WHEN '''' THEN NULL
							ELSE nrcontacorrente
						END numerocontasalario,
						CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
							WHEN ''-'' THEN RIGHT(funcionario.nrcontacorrente,1)
							ELSE NULL
						END numerocontasalariodv,
						NULL numerocontafgts,
						CASE WHEN funcionario.conta_tipo = 1 AND nrcontacorrente = '''' THEN ''D''
						     WHEN funcionario.conta_tipo = 1 THEN ''CC''
						     WHEN funcionario.conta_tipo = 2 THEN ''CP''
						     WHEN funcionario.conta_tipo = 3 THEN ''CS''
						     ELSE ''D''
						END tiporecebimentosalario,
						NULL dataopcaofgts,
						NULL numeroreciborescisao,
						funcionario.dtdemissao::DATE datarescisao,
						NULL tiporeintegracao,
						NULL numeroleianistia,
						NULL datareintegracaoretroativa,
						NULL datareintegracaoretorno,
						COALESCE(socios.sexo,funcionario.sexo) sexo,
						CASE socios.dsestadocivil
							WHEN ''C'' THEN ''C''
							WHEN ''U'' THEN ''U''
							WHEN ''S'' THEN ''S''
							ELSE NULL
						END estadocivil,
						CASE
							WHEN COALESCE(socios.dtnascimento,funcionario.dtnascimento) IS NULL THEN ''01/01/1999''
							ELSE COALESCE(socios.dtnascimento,funcionario.dtnascimento)::VARCHAR
						END datanascimento,
						COALESCE(socios.uf_municipio_nascimento,funcionario.uf_municipio_nascimento) ufnascimento,
						municipinascimento.nmmunicipio cidadenascimento,
						COALESCE(socios.pai_nome,funcionario.pai_nome) nomepai,
						COALESCE(socios.mae_nome,funcionario.mae_nome) nomemae,
						COALESCE(socios.estrangeiro_data_chegada,funcionario.estrangeiro_data_chegada) datachegadapais,
						NULL datanaturalizacao,
						COALESCE(socios.estrangeiro_casado_brasileiro,funcionario.estrangeiro_casado_brasileiro) casadocombrasileiro,
						COALESCE(socios.estrangeiro_filhos_brasileiros,funcionario.estrangeiro_filhos_brasileiros) filhobrasileiro,
						NULL /*socios.deficiente_tipo*/ deficientevisual,
						NULL /*socios.deficiente_tipo*/ deficienteauditivo,
						CASE COALESCE(socios.deficiente_reabilitado,funcionario.deficiente_reabilitado) WHEN TRUE THEN ''S'' ELSE NULL END reabilitado,
						COALESCE(socios.ctps_numero,funcionario.nrctps) numeroctps,
						COALESCE(socios.ctps_serie,funcionario.srctps) seriectps,
						COALESCE(socios.ctps_uf,funcionario.ufctps) ufctps,
						NULL dataexpedicaoctps,
						NULL numeroric,
						NULL orgaoemissorric,
						NULL dataexpedicaoric,
						NULL ufric,
						NULL cidaderic,
						REGEXP_REPLACE(COALESCE(socios.nridentidade,funcionario.nridentidade),''[^0-9]'','''',''g'') numerorg,
						socios.nmorgaoexp orgaoemissorrg,
						COALESCE(socios.dtemissaorg,funcionario.identidade_expedicao)::DATE dataexpedicaorg,
						NULL ufrg,
						funcionario.orgao_classe_inscricao numerooc,
						funcionario.orgao_classe_emissor orgaoemissoroc,
						funcionario.orgao_classe_data_expedicao dataexpedicaooc,
						funcionario.orgao_classe_data_validade datavalidadeoc,
						COALESCE(socios.cnh_numero,funcionario.cnh_numero) numerocnh,
						COALESCE(socios.cnh_orgao_emissor,funcionario.cnh_orgao_emissor) orgaoemissorcnh,
						COALESCE(socios.cnh_expedicao,funcionario.cnh_expedicao)::DATE dataexpedicaocnh,
						COALESCE(socios.cnh_data_validade,funcionario.cnh_data_validade)::DATE datavalidadecnh,
						funcionario.cnh_data_primeira_emissao::DATE dataprimeirahabilitacaocnh,
						COALESCE(socios.cnh_categoria,funcionario.cnh_categoria) categoriacnh,
						NULL numeropassaporte,
						NULL orgaoemissorpassaporte,
						NULL ufpassaporte,
						NULL dataexpedicaopassaporte,
						NULL datavalidadepassaporte,
						COALESCE(socios.nrcpf,funcionario.nrcpf) cpf,
						NULL nis,
						NULL numeronaturalizacao,
						NULL numerote,
						NULL zonate,
						NULL secaote,
						NULL ufte,
						NULL numeroatestadoobito,
						CASE
							WHEN funcionario.dtdemissao IS NOT NULL THEN funcionario.dtdemissao::DATE
							WHEN (EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || EXTRACT(MONTH FROM funcionario.dtadmissao) || ''-'' || EXTRACT(DAY FROM funcionario.dtadmissao))::DATE <= now()::DATE AND EXTRACT(MONTH FROM funcionario.dtadmissao) = 2 AND EXTRACT(DAY FROM funcionario.dtadmissao) = 29 THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-'' || ''02-28'')::DATE
							WHEN (EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || EXTRACT(MONTH FROM funcionario.dtadmissao) || ''-'' || EXTRACT(DAY FROM funcionario.dtadmissao))::DATE <= now()::DATE THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-'' || RIGHT(''0'' || EXTRACT(MONTH FROM funcionario.dtadmissao)::VARCHAR(2),2) || ''-'' || RIGHT(''0'' || EXTRACT(DAY FROM funcionario.dtadmissao)::VARCHAR(2),2))::DATE
							WHEN EXTRACT(MONTH FROM funcionario.dtadmissao) = 2 AND EXTRACT(DAY FROM funcionario.dtadmissao) = 29 THEN ((EXTRACT(YEAR FROM now())+1)::VARCHAR(4) || ''-02-28'')::DATE
							ELSE coalesce(coalesce((EXTRACT(YEAR FROM now())::VARCHAR(4) || ''-'' || RIGHT(''0'' || EXTRACT(MONTH FROM funcionario.dtadmissao)::VARCHAR(2),2) || ''-'' || RIGHT(''0'' || EXTRACT(DAY FROM funcionario.dtadmissao)::VARCHAR(2),2))::DATE,funcionario.dtadmissao + INTERVAL ''365 days''),''01/01/1901'')
						END datavencimentoatestadomedico,
						NULL tipocertidao,
						NULL numerocertidao,
						NULL livrocertidao,
						NULL folhacertidao,
						NULL dataexpedicaocertidao,
						NULL cidadecertidao,
						NULL ufcertidao,
						NULL cartoriocertidao,
						NULL numerocr,
						NULL dataexpedicaocr,
						NULL seriecr,
						REPLACE(COALESCE(socios.dsendereco,funcionario.dsendereco), '';'', '''')  logradouro,
						REPLACE(COALESCE(socios.endereco_numero,funcionario.endereco_numero), '';'', '''') numero,
						REPLACE(COALESCE(socios.endereco_complemento,funcionario.endereco_complemento), '';'', '''') complemento,
						COALESCE(socios.nmbairro,funcionario.nmbairro) bairro,
						COALESCE(socios.nmcidade,funcionario.nmcidade) cidade,
						REPLACE(COALESCE(socios.nrcep,funcionario.nrcep),''-'','''') cep,
						COALESCE(socios.nrtelefone,funcionario.nrtelefone) telefone,
						COALESCE(socios.nmemail,socios.usuarioswmail) email,
						NULL saldoferias,
						coalesce(COALESCE(funcferias.dtperiodoaquisinicial,funcionario.dtadmissao),''01/01/1900'')::DATE inicioperiodoaquisitivoferias,
						NULL dataproximasferias,
						NULL saldofgts,
						NULL percentualadiantamento,
						funcionario.stinss descontainss,
						NULL tinhaempregonoaviso,
						NULL sindicalizado,
						NULL datainicioanuenio,
						NULL jornadareduzida,
						NULL teveavisoindenizado,
						NULL fgtsmesanteriorrescisaorecolhi,
						NULL numerocartaovt,
						NULL diasemanacommeiovt,
						REGEXP_REPLACE(COALESCE(socios.dsobs,funcionario.dsobservacao), ''[\n\r]+'', '''', ''g'' ) observacao,
						NULL cnpjempresaanterior,
						NULL matriculaempresaanterior,
						NULL dataadmissaoempresaanterior,
						NULL cnpjcendente,
						NULL matriculacedente,
						NULL dataadmissaocedente,
						NULL cnpjempresasucessora,
						NULL trabalhaemoutraempresa,
						NULL dddtel,
						NULL sangue,
						NULL ordemcalculoduplovinculo,
						NULL deficienteintelectual,
						NULL deficientemental,
						NULL motivoadmissao,
						NULL quantidademediahorassemanais,
						NULL motivocontratacaotemporaria,
						NULL avisoindenizadopago,
						NULL dataprojecaoavisopreviopago,
						NULL recolheufgtsmesanterior,
						NULL regimedejornada,
						NULL mesestrabalhadosoutrasempresas,
						NULL deficientefisico,
						NULL jornadacumpridasemanademissao,
						NULL jornadasabadocompensadasemanad,
						NULL cageddiariogerado,
						NULL nivelestagio,
						NULL areaatuacaoestagio,
						NULL apoliceseguroestagio,
						NULL nomesupervisorestagio,
						NULL cpfsupervisorestagio,
						NULL estagioobrigatorio,
						NULL percentualdeducaobaseirrf,
						NULL percentualdeducaobaseinss,
						NULL aliquotaiss,
						NULL motivodesligamentodiretor,
						NULL datatransferencia,
						NULL valorgratificacoesrescisao,
						NULL valorbancohorasrescisao,
						NULL mesesbancohorasrescisao,
						NULL mesesgratificacoesrescisao,
						NULL excluido,
						NULL celular,
						NULL dddcel,
						null ufcnh,
						null matriculaesocial,
						empresa.cdempresa estabelecimento,
						funcionario.categoria_esocial categoriatrabalhador,
						NULL agentenocivo,
						CASE
							WHEN cargos.novocodigo=''0377'' THEN ''3547''
							ELSE COALESCE(funcoesb.nrcbonovo,''421205'') 
						END cbo,
						CASE
							WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN ''PADRAO''
							WHEN depto.cdchamada is not null then depto.cdchamada
							else ''PADRAO''
						END  departamento,
						CASE COALESCE(socios.codigo_instrucao,funcionario.codigo_instrucao)
							WHEN ''1'' THEN ''01''
							WHEN ''2'' THEN ''02''
							WHEN ''3'' THEN ''03''
							WHEN ''4'' THEN ''04''
							WHEN ''5'' THEN ''05''
							WHEN ''6'' THEN ''06''
							WHEN ''7'' THEN ''07''
							WHEN ''8'' THEN ''08''
							WHEN ''9'' THEN ''09''
							WHEN ''10'' THEN ''10''
							ELSE ''07''
						END grauinstrucao,
						''00001'' horario,
						CASE
							WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒÆ’Ã†â€™O PODE SER VAZIO''
							                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒÆ’Ã†â€™O PODE SER VAZIO''
							                                                                                                                    ELSE empresa.cdempresa::varchar
							                                                                                                                    END || depto.cdchamada::varchar
							ELSE empresa.cdempresa::VARCHAR
						END lotacao,
						COALESCE(RIGHT(''000'' || cargos.novocodigo::VARCHAR,4),''0001'') cargo,
						COALESCE(funcoesb.cdchamada,''000001'') nivelcargo,
						''PADRAO'' sindicato,
						CASE WHEN
								CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
									WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
									WHEN '''' THEN NULL
									ELSE nrcontacorrente
								END IS NULL THEN NULL
								ELSE COALESCE(agencia.cdchamada,''9999'')
						END agencia,
						CASE WHEN
								CASE LEFT(RIGHT(funcionario.nrcontacorrente,2),1)
									WHEN ''-'' THEN LEFT(funcionario.nrcontacorrente,LENGTH(funcionario.nrcontacorrente)-2)
									WHEN '''' THEN NULL
									ELSE nrcontacorrente
								END IS NULL THEN NULL
								ELSE COALESCE(bancos.cdchamada,''033'')
						END banco,
						NULL funcao,
						CASE
							WHEN funcionario.municipio_nascimento_codigo = 0 THEN NULL
							ELSE funcionario.municipio_nascimento_codigo
						END::VARCHAR  municipionascimento,
						CASE
							WHEN funcionario.endereco_municipio_codigo = 0 THEN NULL
							ELSE funcionario.endereco_municipio_codigo 
						END::VARCHAR municipioresidencia,
						CASE funcionario.pais_nascimento_codigo WHEN ''105'' THEN ''1058'' ELSE NULL END paisnascimento,
						CASE funcionario.endereco_pais_codigo WHEN ''105'' THEN ''1058'' ELSE NULL END paisresidencia,
						CASE socios.esocial_pais_nacionalidade WHEN ''105'' THEN ''1058'' ELSE NULL END paisnacionalidade,
						NULL paisemissaopassaporte,
						CASE COALESCE(socios.raca_cor,funcionario.raca_cor)
							WHEN ''0'' THEN ''I''
							WHEN ''2'' THEN ''B''
							WHEN ''4'' THEN ''N''
							WHEN ''6'' THEN ''A''
							WHEN ''8'' THEN ''P''
							ELSE NULL
						END raca,
						NULL simplesconcomitante,
						NULL tipofuncionario,
						CASE
							WHEN funcionario.endereco_tipo = '''' THEN NULL
							ELSE funcionario.endereco_tipo
						END tipologradouro,
						NULL motivorescisao,
						NULL acordodeprorrogacao,
						NULL instituicaoensino,
						NULL instituicaointegradora,
						NULL processoadmissao,
						NULL processodemissao,
						NULL processoinss,
						NULL processoirrf,
						NULL processomenoraprendiz,
						NULL processoreintegracao,
						''15'' vinculo,
						emp.cdempresa empresa,
						emp.cdempresa grupoempresarial
					FROM wphd.empresa,wphd.empresa emp, wdp.tb' || r.tablename || ' AS funcionario
					INNER JOIN wphd.socios ON funcionario.idsocio = socios.cdsocio
					LEFT JOIN wdp.depto ON funcionario.iddepartamento = depto.iddepartamento
					LEFT JOIN wphd.municipiosibge municipinascimento ON socios.municipio_nascimento_codigo::VARCHAR = municipinascimento.codmunicipio
					LEFT JOIN wdp.funcoesb ON funcionario.funcao_id = funcoesb.idfuncao
					LEFT JOIN cargos ON funcoesb.cdchamada = cargos.codigo
					LEFT JOIN wdp.agencia ON funcionario.idagencia = agencia.idagencia AND funcionario.idbanco = agencia.idbanco
					LEFT JOIN wdp.bancos ON agencia.idbanco = bancos.idbanco
					LEFT JOIN (SELECT DISTINCT ON (idfuncionario) idfuncionario, dtperiodoaquisinicial::DATE FROM wdp.a' || r.tablename || ' ORDER BY idfuncionario, dtperiodoaquisinicial DESC) AS funcferias ON funcionario.identificacontrib::varchar = funcferias.idfuncionario
					WHERE
						(
							(
								LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
							AND
								LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
							AND
								LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
							)
							OR
							(
								LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
							AND
								REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
							)
						)
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	end if;
	END LOOP;

	sqlcmd := sqlcmd || ') TO ''${PASTA_SAIDA}${CLIENTE}\trabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';


	EXECUTE sqlcmd;
END
$$;




-- DEPENDENTES

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'd_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT distinct
					RIGHT(dependente.cdchamada,6) codigo,
					dependente.nmdependente descricao,
					dependente.dtiniciodependencia::DATE datainclusao,
					CASE dependente.tprelacao
						WHEN ''1'' THEN ''F''
						WHEN ''2'' THEN ''CO''
						WHEN ''3'' THEN ''F''
					END tipoparentesco,
					NULL impostorenda,
					NULL salariofamilia,
					NULL pensaoalimenticia,
					NULL percentualpensaoalimentic,
					NULL percentualpensaoalimenticfgts,
					dependente.nrcpf cpf,
					CASE WHEN dependente.dtnascimento::DATE IS NULL THEN ''01/01/1999''
					ELSE dependente.dtnascimento::DATE
					END datanascimento,
					dependente.cdufnascimento ufnascimento,
					dependente.dslocalnascimento cidadenascimento,
					dependente.nrcartorio cartoriocertidao,
					dependente.nrregistro numeroregistrocertidao,
					REPLACE(RIGHT(dependente.nrlivro,10),''"'','''') numerolivrocertidao,
					RIGHT(dependente.nrfolha,5) numerofolhacertidao,
					dependente.dtentregacert::DATE dataentregacertidao,
					dependente.dtbaixa::DATE databaixacertidao,
					NULL tiporecebimento,
					NULL numerocontarecebimento,
					NULL numerocontadvrecebimento,
					NULL datavencimentodeclaracaoe,
					NULL datavencimentovacinacao,
					NULL sexo,
					NULL databaixaimpostorenda,
					NULL planosaude,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6)rabalhador,
					NULL banco,
					NULL agencia,
					NULL dependentetrabalhadorpens,
					NULL eventopensaofolha,
					NULL eventopensaoferias,
					NULL eventopensaopplr,
					NULL eventopensao13,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa,wphd.empresa emp, wdp.d' || r.tablename || ' dependente
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON dependente.idfuncionario = funcionario.idfuncionario
				WHERE
					(
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
						AND
							LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
						AND
							LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
						)
						OR
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
						AND
							REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
						)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	sqlcmd := sqlcmd || ') TO ''${PASTA_SAIDA}${CLIENTE}\dependentestrabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;

-- AFASTAMENTOS

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'a_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT distinct on (emp.cdempresa, empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6), afastamento.dtinicial::DATE)
					NULL codigo,
					afastamento.dsmotivo descricao,
					afastamento.dtinicial::DATE "data",
					COALESCE((afastamento.dtfinal::DATE-afastamento.dtinicial::DATE)+1,999) dias,
					CASE afastamento.idespecial
						WHEN ''0010000001'' THEN ''003''
						WHEN ''ZZZG7E0B0E0A39382820E66AF539C4C802'' THEN ''003''
						WHEN ''0010000002'' THEN ''015''
						WHEN ''0010000003'' THEN ''006''
						WHEN ''0010000004'' THEN ''006''
						WHEN ''0010000006'' THEN ''001''
						WHEN ''0010000008'' THEN ''007''
						WHEN ''0010000009'' THEN ''007''
						WHEN ''0010000007'' THEN ''009''
						WHEN ''001000000D'' THEN ''003''
						WHEN ''001000000E'' THEN ''014''
						WHEN ''ZZZG7E1C040C111811103381D011A0A382'' THEN ''003''
						WHEN ''ZZZG7E1C040C113B2A80359E15F0A0A388'' THEN ''003''
						WHEN ''ZZZG7E1C040C120D1BE036700F18A0A38F'' THEN ''001''
						WHEN ''ZZZG7E1C040C12303940388D4985A0A394'' THEN ''007''
						WHEN ''ZZZG7E1C040C1234181038C243FFA0A397'' THEN ''004''
						WHEN ''ZZZG7E1C040C123523C038D473CEA0A398'' THEN ''002''
						WHEN ''ZZZG7E1C040C1302104039590536A0A39C'' THEN ''030''
						ELSE ''030''
					END tipohistorico,
					NULL observacao,
					NULL cid,
					NULL cnpjempresacessionaria,
					NULL tipoonuscessionaria,
					NULL tipoonussindicato,
					NULL tipoacidentetransito,
					afastamento.dtperiodoaquisinicial::DATE datainicioperiodoaquisitivo,
					CASE WHEN afastamento.nrdiasdireito-afastamento.nrdiasgozado <= 0 THEN NULL ELSE afastamento.nrdiasdireito-afastamento.nrdiasgozado END diassaldoferias,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) trabalhador,
					NULL sindicato,
					NULL medico,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.a' || r.tablename || ' afastamento
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON afastamento.idfuncionario = funcionario.idfuncionario
				INNER JOIN (SELECT
						   idafastamento,
						   idfuncionario,
						   ROW_NUMBER() OVER (PARTITION BY idfuncionario, dtinicial ORDER BY dtfinal DESC) ordem
					    FROM
						   wdp.a' || r.tablename || ') AS afastunico ON afastamento.idafastamento = afastunico.idafastamento AND afastamento.idfuncionario = afastunico.idfuncionario
				WHERE ((LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
				--AND afastamento.dtinicial::DATE is not null
				';
	END LOOP;

	sqlcmd := sqlcmd || ') TO ''${PASTA_SAIDA}${CLIENTE}\afastamentostrabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;

/*
-- REAJUSTES

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'l_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9'
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT DISTINCT ON (emp.cdempresa,(right(''000000'' || funcionario.cdchamada, 6)),reajuste.dtalteracao::DATE)
					NULL codigo,
					reajuste.dsmotivo descricao,
					reajuste.dtalteracao::DATE "data",
					CASE
						WHEN reajuste.cdmotivoreajuste = ''000'' THEN ''P''
						WHEN reajuste.cdmotivoreajuste = ''001'' THEN ''P''
						WHEN reajuste.cdmotivoreajuste = ''01'' THEN ''P''
						WHEN reajuste.cdmotivoreajuste = ''002'' THEN ''S''
						WHEN reajuste.cdmotivoreajuste = ''003'' THEN ''S''
						WHEN reajuste.cdmotivoreajuste = ''004'' THEN ''S''
						WHEN reajuste.cdmotivoreajuste = ''005'' THEN ''S''
						WHEN reajuste.cdmotivoreajuste = ''006'' THEN ''A''
						WHEN reajuste.cdmotivoreajuste IS NULL   THEN ''P''
						ELSE ''P''
					END tipo,
					reajuste.alreajuste::NUMERIC(10,3) percentual,
					reajuste.vlsalario::NUMERIC(10,2) salarioanterior,
					reajuste.vlnovosalario::NUMERIC(10,2) salarionovo,
					reajuste.tpformapgto unidadesalarionovo,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) trabalhador,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.l' || r.tablename || ' reajuste
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON reajuste.idfuncionario = funcionario.idfuncionario
				WHERE reajuste.alreajuste::NUMERIC(10,3) > 0
				AND reajuste.vlnovosalario::NUMERIC(10,2) > 0
				AND ((LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
				AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	sqlcmd := sqlcmd || ' ORDER BY empresa,trabalhador, data, salarionovo) TO ''${PASTA_SAIDA}${CLIENTE}\reajustestrabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;
*/
-- MUDANÃ‡AS

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'fh_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT
					NULL codigo,
					NULL descricao,
					empresa.cdempresa || right(''000000'' || hfuncoes.idfuncionario, 6)::varchar  trabalhador,
					hfuncoes.dtmudanca::DATE datainicial,
					(SELECT dtmudanca::DATE-1 FROM wdp.fh' || r.tablename || ' WHERE idfuncionario = hfuncoes.idfuncionario AND dtmudanca > hfuncoes.dtmudanca ORDER BY dtmudanca::DATE LIMIT 1) datafinal,
					NULL tipocondicao,
					NULL estabelecimento,
					NULL lotacao,
					NULL departamento,
					RIGHT(''000'' || cargos.novocodigo::VARCHAR,4) cargo,
					hfuncoes.cdchamada nivelcargo,
					NULL horario,
					NULL funcao,
					NULL numerohorasmensais,
					NULL simplescomcomitante,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.fh' || r.tablename || ' hfuncoes
				INNER JOIN cargos ON hfuncoes.cdchamada = cargos.codigo
				WHERE (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 't_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT
					NULL codigo,
					NULL descricao,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada,6) trabalhador,
					hdeptos.dtmudanca::DATE datainicial,
					(SELECT dtmudanca::DATE-1 FROM wdp.t' || r.tablename || ' WHERE idfuncionario = hdeptos.idfuncionario AND dtmudanca > hdeptos.dtmudanca ORDER BY dtmudanca::DATE LIMIT 1) datafinal,
					NULL tipocondicao,
					empresa.cdempresa::TEXT estabelecimento,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					NULL departamento,
					NULL cargo,
					NULL nivelcargo,
					NULL horario,
					NULL funcao,
					NULL numerohorasmensais,
					NULL simplescomcomitante,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.t' || r.tablename || ' hdeptos
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON hdeptos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.depto ON hdeptos.idsetoratual = depto.iddepartamento
				WHERE (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 't_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT
					NULL codigo,
					NULL descricao,
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) trabalhador,
					hdeptos.dtmudanca::DATE datainicial,
					(SELECT dtmudanca::DATE-1 FROM wdp.t' || r.tablename || ' WHERE idfuncionario = hdeptos.idfuncionario AND dtmudanca > hdeptos.dtmudanca ORDER BY dtmudanca::DATE LIMIT 1) datafinal,
					NULL tipocondicao,
					empresa.cdempresa::varchar estabelecimento,
					NULL lotacao,
					depto.cdchamada departamento,
					NULL cargo,
					NULL nivelcargo,
					NULL horario,
					NULL funcao,
					NULL numerohorasmensais,
					NULL simplescomcomitante,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.t' || r.tablename || ' hdeptos
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON hdeptos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.depto ON hdeptos.idsetoratual = depto.iddepartamento
				WHERE (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	sqlcmd := sqlcmd || ') TO ''${PASTA_SAIDA}${CLIENTE}\mudancastrabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''UTF8'';';

	EXECUTE sqlcmd;
END
$$;

DROP TABLE IF EXISTS cargos;

-- FALTAS

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'f_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) trabalhador,
					''IMPORTACAO DA ALTERDATA'' descricao,
					faltasp.dtfalta::DATE "data",
					''J'' tipo,
					''N'' descontaponto,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.faltasp
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON faltasp.idfuncionario = funcionario.idfuncionario
				WHERE (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
				
	END LOOP;

	sqlcmd := sqlcmd || ') TO ''${PASTA_SAIDA}${CLIENTE}\faltas.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;

-- EVENTOS

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	CREATE TEMP TABLE eventos(codigo varchar, descricao varchar, tipovalor varchar, tipocalculo varchar, unidade varchar, categoria varchar, tipomedia varchar, incideinss varchar, incideirrf varchar, incidefgts varchar, totalizarais varchar, totalizainforme varchar, acumulahoraextra varchar, incidepis varchar, incideencargos varchar, fazproporcaopiso varchar, incidedsr varchar, empresa varchar, grupoempresarial varchar);
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'r_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		sqlcmd := 'INSERT INTO eventos ';

		sqlcmd:=sqlcmd || 'SELECT DISTINCT ON (evento.cdchamada)
					''I'' || evento.cdchamada codigo,
					evento.nmevento descricao,
					CASE evento.sttipo
						WHEN ''A'' THEN ''0''
						WHEN ''D'' THEN ''1''
					END tipovalor,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''2''
						WHEN ''2'' THEN ''2''
						WHEN ''A'' THEN ''0''
						WHEN ''F'' THEN ''1''
						WHEN ''M'' THEN ''0''
						WHEN ''R'' THEN ''0''
					END tipocalculo,
					''2'' unidade,
					''0'' categoria,
					NULL tipomedia,
					evento.stcomputainss incideinss,
					evento.stcomputairrf incideirrf,
					evento.stcomputafgts incidefgts,
					evento.stcomputarais totalizarais,
					NULL totalizainforme,
					NULL acumulahoraextra,
					evento.stcomputapis incidepis,
					NULL incideencargos,
					NULL fazproporcaopiso,
					NULL incidedsr,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wphd.empresa, wphd.empresa emp, wdp.evento
				INNER JOIN wdp.r' || r.tablename || ' calculos ON evento.idevento = calculos.idevento
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				WHERE (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				       )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
				
		EXECUTE sqlcmd;
	END LOOP;

	COPY (SELECT DISTINCT ON (grupoempresarial, empresa, codigo) * FROM eventos ORDER BY grupoempresarial, empresa, codigo) TO '${PASTA_SAIDA}${CLIENTE}\rubricas.csv' WITH CSV HEADER DELIMITER ';' ENCODING 'UTF8';

	DROP TABLE eventos;

END
$$;

-- CALCULOS

--AND LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001'' AND LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8) AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''

DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'r_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) funcionario,
					NULL estabelecimento,
					CASE evento.cdchamada
						WHEN ''506'' THEN ''0076''
						WHEN ''507'' THEN ''0031''
						WHEN ''511'' THEN ''0076''
						WHEN ''916'' THEN ''0064''
						WHEN ''917'' THEN ''0076''
						WHEN ''918'' THEN ''0031''
						WHEN ''919'' THEN ''0031''
						WHEN ''927'' THEN ''0076''
						WHEN ''515'' THEN ''0013''
						WHEN ''613'' THEN ''0085''
						ELSE ''I'' || evento.cdchamada
					END rubrica,
					EXTRACT(YEAR FROM calculos.dtpagamento::DATE) ano,
					EXTRACT(MONTH FROM calculos.dtpagamento::DATE) mes,
					''0'' semana,
					calculos.dtpagamento::DATE datapagamento,
					CASE
						WHEN evento.nmevento LIKE ''%INSS%'' THEN replace(calculos.alinss::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN evento.nmevento LIKE ''%IRRF%'' THEN replace(calculos.alirrf::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN calculos.qtevento <= 0 THEN NULL
						ELSE replace(calculos.qtevento::NUMERIC(15,2)::VARCHAR,''.'','','')
					END referencia,
					calculos.vlevento::NUMERIC(15,2) valor,
					CASE calculos.stpagamento WHEN ''N'' THEN ''TRUE'' ELSE ''FALSE'' END invisivel,
					EXTRACT(YEAR FROM historicos.dtprocessamento) anogerador,
					EXTRACT(MONTH FROM historicos.dtprocessamento) mesgerador,
					''0'' semanageradora,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
                                                WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END tipo,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
						WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END calculogerador,
					NULL tipoperiodo,
					NULL mesperiodo,
					NULL semanaperiodo,
					NULL datainicialperiodo,
					NULL datafinalperiodo,
					NULL mesinicialperiodo,
					NULL mesfinalperiodo,
					NULL anoinicialperiodo,
					NULL anofinalperiodo,
					NULL calculanofim,
					NULL conteudo,
					NULL tipoprocedencia,
					NULL tipomovimento,
					calculos.vlevento::NUMERIC(15,2) valorbruto,
					NULL dependentefuncionario,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wdp.r' || r.tablename || ' calculos
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON calculos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.evento ON calculos.idevento = evento.idevento
				INNER JOIN wdp.depto ON calculos.iddepartamento = depto.iddepartamento
				INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa
				INNER JOIN wphd.empresa emp ON (
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
						AND
							LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
						AND
							LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
						)
						OR
						(
							LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
						AND
							REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
						)
					       )
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
				WHERE calculos.vlevento::NUMERIC(15,2) > 0

				UNION ALL

				SELECT
				        empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) funcionario,
					NULL estabelecimento,
					CASE evento.cdchamada
						WHEN ''506'' THEN ''0072'' -- FGTS RescisÃ£o
						WHEN ''507'' THEN ''0022'' -- FGTS 13o. SalÃ¡rio RescisÃ£o
						WHEN ''511'' THEN ''0072'' -- FGTS MÃªs Anterior
						WHEN ''899'' THEN ''0021'' -- INSS 13o. SalÃ¡rio
						WHEN ''900'' THEN ''0025'' -- INSS  RescisÃ£o
						WHEN ''901'' THEN ''0021'' -- INSS 13o. RescisÃ£o
						WHEN ''902'' THEN ''0044'' -- INSS FÃ©rias
						WHEN ''903'' THEN ''0025'' -- INSS Folha
						WHEN ''909'' THEN ''0020'' -- IRRF 13o. SalÃ¡rio
						WHEN ''910'' THEN ''0073'' -- IRRF RescisÃ£o
						WHEN ''911'' THEN ''0020'' -- IRRF 13o. RescisÃ£o
						WHEN ''913'' THEN ''0045'' -- IRRF FÃ©rias
						WHEN ''914'' THEN ''0073'' -- IRRF Folha
						WHEN ''916'' THEN ''0046'' -- FGTS em FÃ©rias
						WHEN ''917'' THEN ''0072'' -- FGTS em Folha
						WHEN ''918'' THEN ''0022'' -- FGTS 13Âº SalÃ¡rio (Adiantamento)
						WHEN ''919'' THEN ''0022'' -- FGTS 13Âº SÃ¡lario
						WHEN ''921'' THEN ''0072'' -- DiferenÃ§a de FGTS
						WHEN ''925'' THEN ''0025'' -- INSS DiferenÃ§as Salariais
						WHEN ''927'' THEN ''0072'' -- FGTS DiferenÃ§as Salariais
					END rubrica,
					EXTRACT(YEAR FROM calculos.dtpagamento::DATE) ano,
					EXTRACT(MONTH FROM calculos.dtpagamento::DATE) mes,
					''0'' semana,
					calculos.dtpagamento::DATE datapagamento,
					CASE
						WHEN evento.nmevento LIKE ''%INSS%'' THEN replace(calculos.alinss::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN evento.nmevento LIKE ''%IRRF%'' THEN replace(calculos.alirrf::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN calculos.qtevento <= 0 THEN NULL
						ELSE replace(calculos.qtevento::NUMERIC(15,2)::VARCHAR,''.'','','')
					END referencia,
					calculos.vlbaseevento::NUMERIC(15,2) valor,
					''TRUE'' invisivel,
					EXTRACT(YEAR FROM historicos.dtprocessamento) anogerador,
					EXTRACT(MONTH FROM historicos.dtprocessamento) mesgerador,
					''0'' semanageradora,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
                                                WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END tipo,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
                                                WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END calculogerador,
					NULL tipoperiodo,
					NULL mesperiodo,
					NULL semanaperiodo,
					NULL datainicialperiodo,
					NULL datafinalperiodo,
					NULL mesinicialperiodo,
					NULL mesfinalperiodo,
					NULL anoinicialperiodo,
					NULL anofinalperiodo,
					NULL calculanofim,
					NULL conteudo,
					NULL tipoprocedencia,
					NULL tipomovimento,
					CASE WHEN calculos.vlbaseeventobruto::NUMERIC(15,2) <= 0 THEN calculos.vlbaseevento::NUMERIC(15,2) ELSE vlbaseeventobruto::NUMERIC(15,2) END valorbruto,
					NULL dependentefuncionario,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wdp.r' || r.tablename || ' calculos
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON calculos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.evento ON calculos.idevento = evento.idevento
				INNER JOIN wdp.depto ON calculos.iddepartamento = depto.iddepartamento
				INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa
				INNER JOIN wphd.empresa emp ON (
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
									AND
										LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
									AND
										LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
									)
									OR
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
									AND
										REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
									)
								)
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
				WHERE calculos.vlbaseevento::NUMERIC(15,2) > 0
				AND evento.cdchamada IN (''506'',''507'',''511'',''899'',''900'',''901'',''902'',''903'',''909'',''910'',''911'',''913'',''914'',''916'',''917'',''918'',''919'',''921'',''925'',''927'')
                                
				UNION ALL

				SELECT
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) funcionario,
					NULL estabelecimento,
					CASE evento.cdchamada
						WHEN ''899'' THEN ''0028'' -- INSS 13o. SalÃ¡rio
						WHEN ''900'' THEN ''0026'' -- INSS  RescisÃ£o
						WHEN ''901'' THEN ''0028'' -- INSS 13o. RescisÃ£o
						WHEN ''902'' THEN ''0047'' -- INSS FÃ©rias
						WHEN ''903'' THEN ''0026'' -- INSS Folha
						WHEN ''925'' THEN ''0026'' -- INSS DiferenÃ§as Salariais
					END rubrica,
					EXTRACT(YEAR FROM calculos.dtpagamento::DATE) ano,
					EXTRACT(MONTH FROM calculos.dtpagamento::DATE) mes,
					''0'' semana,
					calculos.dtpagamento::DATE datapagamento,
					CASE
						WHEN evento.nmevento LIKE ''%INSS%'' THEN replace(calculos.alinss::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN evento.nmevento LIKE ''%IRRF%'' THEN replace(calculos.alirrf::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN calculos.qtevento <= 0 THEN NULL
						ELSE replace(calculos.qtevento::NUMERIC(15,2)::VARCHAR,''.'','','')
					END referencia,
					calculos.vlbaseevento::NUMERIC(15,2) valor,
					''TRUE'' invisivel,
					EXTRACT(YEAR FROM historicos.dtprocessamento) anogerador,
					EXTRACT(MONTH FROM historicos.dtprocessamento) mesgerador,
					''0'' semanageradora,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
                                                WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END tipo,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
						WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END calculogerador,
					NULL tipoperiodo,
					NULL mesperiodo,
					NULL semanaperiodo,
					NULL datainicialperiodo,
					NULL datafinalperiodo,
					NULL mesinicialperiodo,
					NULL mesfinalperiodo,
					NULL anoinicialperiodo,
					NULL anofinalperiodo,
					NULL calculanofim,
					NULL conteudo,
					NULL tipoprocedencia,
					NULL tipomovimento,
					CASE WHEN calculos.vlbaseeventobruto::NUMERIC(15,2) <= 0 THEN calculos.vlbaseevento::NUMERIC(15,2) ELSE vlbaseeventobruto::NUMERIC(15,2) END valorbruto,
					NULL dependentefuncionario,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wdp.r' || r.tablename || ' calculos
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON calculos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.evento ON calculos.idevento = evento.idevento
				INNER JOIN wdp.depto ON calculos.iddepartamento = depto.iddepartamento
				INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa
				INNER JOIN wphd.empresa emp ON (
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
									AND
										LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
									AND
										LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
									)
									OR
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
									AND
										REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
									)
								)
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
				WHERE calculos.vlbaseevento::NUMERIC(15,2) > 0
				AND evento.cdchamada IN (''899'',''900'',''901'',''902'',''903'',''925'')

				UNION ALL

				SELECT
					empresa.cdempresa || right(''000000'' || funcionario.cdchamada, 6) funcionario,
					NULL estabelecimento,
					''0075'' rubrica,
					EXTRACT(YEAR FROM calculos.dtpagamento::DATE) ano,
					EXTRACT(MONTH FROM calculos.dtpagamento::DATE) mes,
					''0'' semana,
					calculos.dtpagamento::DATE datapagamento,
					CASE
						WHEN evento.nmevento LIKE ''%INSS%'' THEN replace(calculos.alinss::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN evento.nmevento LIKE ''%IRRF%'' THEN replace(calculos.alirrf::NUMERIC(15,2)::VARCHAR,''.'','','')
						WHEN calculos.qtevento <= 0 THEN NULL
						ELSE replace(calculos.qtevento::NUMERIC(15,2)::VARCHAR,''.'','','')
					END referencia,
					calculos.vlbaseevento::NUMERIC(15,2) valor,
					''TRUE'' invisivel,
					EXTRACT(YEAR FROM historicos.dtprocessamento) anogerador,
					EXTRACT(MONTH FROM historicos.dtprocessamento) mesgerador,
					''0'' semanageradora,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
                                                WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END tipo,
					CASE historicos.tpprocessamento
						WHEN ''1'' THEN ''Ad13''
						WHEN ''2'' THEN ''13''
						WHEN ''A'' THEN ''Ad''
						WHEN ''F'' THEN ''Fe''
						WHEN ''M'' THEN ''Fo''
						WHEN ''R'' THEN ''Re''
						WHEN ''H'' THEN ''Fo''
						ELSE historicos.tpprocessamento
					END calculogerador,
					NULL tipoperiodo,
					NULL mesperiodo,
					NULL semanaperiodo,
					NULL datainicialperiodo,
					NULL datafinalperiodo,
					NULL mesinicialperiodo,
					NULL mesfinalperiodo,
					NULL anoinicialperiodo,
					NULL anofinalperiodo,
					NULL calculanofim,
					NULL conteudo,
					NULL tipoprocedencia,
					NULL tipomovimento,
					CASE WHEN calculos.vlbaseeventobruto::NUMERIC(15,2) <= 0 THEN calculos.vlbaseevento::NUMERIC(15,2) ELSE vlbaseeventobruto::NUMERIC(15,2) END valorbruto,
					NULL dependentefuncionario,
					CASE
						WHEN REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') <> REGEXP_REPLACE(depto.nrcgc,''[^0-9]'','''',''g'') THEN CASE WHEN empresa.cdempresa IS NULL THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    WHEN empresa.cdempresa::varchar = '''' ::varchar THEN ''LOTACAO NÃƒO PODE SER VAZIO''
						                                                                                                                    ELSE empresa.cdempresa::varchar
						                                                                                                                    END || depto.cdchamada::varchar
						ELSE empresa.cdempresa::VARCHAR
					END lotacao,
					emp.cdempresa empresa,
					emp.cdempresa grupoempresarial
				FROM wdp.r' || r.tablename || ' calculos
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON calculos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.evento ON calculos.idevento = evento.idevento
				INNER JOIN wdp.depto ON calculos.iddepartamento = depto.iddepartamento
				INNER JOIN wphd.empresa ON depto.idempresa::INTEGER = empresa.cdempresa
				INNER JOIN wphd.empresa emp ON (
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
									AND
										LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
									AND
										LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
									)
									OR
									(
										LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
									AND
										REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
									)
								)
					AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''
				WHERE calculos.vlbaseevento::NUMERIC(15,2) > 0
				AND evento.cdchamada = ''903''';

				
	END LOOP;

	sqlcmd := sqlcmd || ' ORDER BY 15) TO ''${PASTA_SAIDA}${CLIENTE}\calculostrabalhadores.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;

/*

CALCULOS TRABALHADORES TIPOS


DO
$$
DECLARE r RECORD;
DECLARE sqlcmd TEXT;
BEGIN

	sqlcmd := 'COPY (';
	
	FOR r IN (
			SELECT
				RIGHT(tablename,5) tablename
			FROM pg_tables
			WHERE schemaname = 'wdp'
			AND tablename LIKE 'r_____'
			AND RIGHT(tablename,1) BETWEEN '0' AND '9' 			AND RIGHT(tablename,5)::integer in (select cdempresa from wphd.empresa)
		 )
	LOOP

		IF (sqlcmd <> 'COPY (') THEN
			sqlcmd := sqlcmd || ' UNION ';
		END IF;

		sqlcmd:=sqlcmd || 'SELECT DISTINCT
					empresa.cdempresa,
					historicos.tpprocessamento
				FROM wphd.empresa, wphd.empresa emp, wdp.r' || r.tablename || ' calculos
				INNER JOIN wdp.h' || r.tablename || ' historicos ON calculos.idhistorico = historicos.idhistorico
				INNER JOIN wdp.f' || r.tablename || ' funcionario ON calculos.idfuncionario = funcionario.idfuncionario
				INNER JOIN wdp.evento ON calculos.idevento = evento.idevento
				INNER JOIN wdp.depto ON calculos.iddepartamento = depto.iddepartamento
				WHERE calculos.vlbaseevento::NUMERIC(15,2) > 0
				AND (
					(
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 14
					AND
						LEFT(RIGHT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),6),4) = ''0001''
					AND
						LEFT(REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g''),8) =  LEFT(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g''),8)
					)
				        OR
				        (
						LENGTH(REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'')) = 11
					AND
						REGEXP_REPLACE(emp.nrcgc,''[^0-9]'','''',''g'') = REGEXP_REPLACE(empresa.nrcgc,''[^0-9]'','''',''g'')
					)
				    )
				AND empresa.cdempresa = ''' || r.tablename::INTEGER || '''';
	END LOOP;

	sqlcmd := sqlcmd || ' ORDER BY 1,2) TO ''${PASTA_SAIDA}${CLIENTE}\calculostrabalhadorestipos.csv'' WITH CSV HEADER DELIMITER '';'' ENCODING ''WIN1252'';';

	EXECUTE sqlcmd;
END
$$;
*/


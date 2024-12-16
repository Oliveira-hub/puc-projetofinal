# Nome do projeto

#### Aluno: João Gabriel Evelin D'Oliveira (https://github.com/Oliveira-hub)
#### Orientador: Anderson Silva

---

Trabalho apresentado ao curso [BI MASTER](https://ica.puc-rio.ai/bi-master) como pré-requisito para conclusão de curso e obtenção de crédito na disciplina "Projetos de Sistemas Inteligentes de Apoio à Decisão".

- [Link para o código](https://github.com/Oliveira-hub/puc-projetofinal). 

---

### Resumo

Este trabalho foi elaborado dentro do TJRJ e utilizado como projeto de conclusão de Curso para o BI-Master da PUC-Rio. 

Indicadores CNJ são indicadores de produtividade dos magistrados dos Tribunais de Justiça diante de critérios do Conselho Nacional de Justiça. Este projeto aborda tais indicadores para segunda instância onde atuam desembargadores e contempla os seguintes indicadores: casos novos, baixados, decisões, pendentes e suspensos.


### 1. Introdução

A Sala Íris funciona como uma central de dados abrangente no TJ-RJ, reunindo em um único ponto de acesso todas as bases de dados do Judiciário fluminense, tanto judiciais quanto administrativas. Essa integração permite que magistrados obtenham indicadores de forma rápida e eficiente, facilitando a tomada de decisões.
Os indicadores de desempenho (Indicadores CNJ) servem como ferramentas para apoiar os órgãos do Judiciário na busca pelos Macrodesafios estabelecidos. Eles desempenham um papel essencial ao monitorar a implementação da Estratégia Nacional do Poder Judiciário.
Os magistrados, interessados em analisar tais indicadores elaborados pelo CNJ (Conselho Nacional de Justiça), pediram para que fosse desenvolvido um painel na ferramenta Qlik (Ferramenta de visualização de relatórios) que substituísse os antigos relatórios gerados pelos sistemas. Os sistemas são: PJE, DCP e EJUD. 

Além disso, deve ser possível monitorar os painéis com cargas diárias, visto que os relatórios anteriores eram mensais.
Este projeto abordará os Indicadores CNJ da segunda instância, referentes aos processos eletrônicos registrados no sistema EJUD.
A primeira instância e a segunda instância são níveis do Poder Judiciário responsáveis por analisar e julgar processos em etapas distintas, com funções e competências diferentes. Quem julga os processos da segunda instância são os desembargadores (nos Tribunais de Justiça ou Tribunais Regionais).

A proposta é juntar as informações disponíveis no modelo transacional do sistema EJUD, extrair e abstrair informações para que seja possível elaborar estratégias futuras.

É desejável que esse painel tenha os seguintes indicadores: Casos Novos, Baixados, Decisões, Suspensos e Pendentes.

Além disso, deve ser distinguido quais magistrados foram os relatores e seus respectivos órgãos julgadores, levando em conta o período disponibilizado.

Ao final do projeto de Indicadores CNJ da Segunda Instância, além da construção das consultas que incorporam as regras de negócio por trás de cada indicador e automatização do cálculo desses indicadores, será desenvolvido um dashboard com os dados mais relevantes por um Analista


### 2. Modelagem

O projeto do ETL foi feito proceduralmente e dividido em três fases: Carga e classificação de movimentos, Geração de indicadores, Orquestração.
Fases:

Carga e classificação de movimentos:
    p_mov_distribuicao
    p_mov_baixa
    p_mov_decisao
    p_mov_suspensao
    p_mov_saida_suspensao

Geração de Indicadores:
    p_ejud_casosnovos
    p_ejud_baixados
    p_ejud_decisoes
    p_ejud_pendentes
    p_ejud_suspensos

Orquestração:
    p_ejud_mpm
    p_ejud_mpm_basededados
    p_ejud_mpm_indicadores


### 3. Resultados e Conclusões

O maior trabalho desse projeto foi entender as regras de negócio para classificação dos movimentos citados anteriormente. Após a classificação desses movimentos ainda foram calculados os indicadores usando um glossário contendo instruções de como calcular os indicadores.

O projeto foi realizado por um analista e um engenheiro de dados. A parte de engenharia foi abordada no escopo do projeto, deixando análises de relacionamentos das informações para o analista.

De modo geral conseguimos trazer os requisitos levantados pela secretaria e disponíveis e elaborar um dashboard objetivo com as informações desejadas.

Vale a pena levantar que a dimensão de data foi gerada artificialmente a partir dos campos ano e mês. 


---

Matrícula: 212.100.370

Pontifícia Universidade Católica do Rio de Janeiro

Curso de Pós Graduação *Business Intelligence Master*
# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import zipfile
import re
from sklearn.preprocessing import LabelEncoder
import types


def STRIP(text):

    try:
        return text.strip().replace(u"\x00", '')
    except AttributeError:
        return text

class RemuneracaoFederalExtractor:

    CAD_COLUMNS = ['Id_SERVIDOR_PORTAL', 'NOME', 'DESCRICAO_CARGO',
                   'CLASSE_CARGO', 'REFERENCIA_CARGO', 'PADRAO_CARGO', 'NIVEL_CARGO',
                   'NIVEL_FUNCAO', 'FUNCAO',
                   'ATIVIDADE', 'UORG_LOTACAO',
                   'ORG_LOTACAO', 'ORGSUP_LOTACAO', 'UORG_EXERCICIO',
                   'ORG_EXERCICIO',
                   'ORGSUP_EXERCICIO', 'TIPO_VINCULO', 'SITUACAO_VINCULO',
                   'DATA_INICIO_AFASTAMENTO', 'DATA_TERMINO_AFASTAMENTO',
                   'REGIME_JURIDICO', 'JORNADA_DE_TRABALHO', 'DATA_INGRESSO_CARGOFUNCAO',
                   'DATA_NOMEACAO_CARGOFUNCAO', 'DATA_INGRESSO_ORGAO',
                   'DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO']

    CAD_READ_CFG = {
        "sep": "\t",
        "encoding": "iso-8859-1", 
        "decimal": ",",
        #"usecols": CAD_COLUMNS,
        "iterator": True,
        "chunksize": 10000,
        #"converters": {column_name:STRIP for column_name in CAD_COLUMNS},
        # workround > n conseguindo ler as colunas por conta do \x000
        "header": None
    }

    CAD_CAT_COLS = ['NOME',
                'DESCRICAO_CARGO',
                'UORG_LOTACAO',
                'ORG_LOTACAO',
                'ORGSUP_LOTACAO',
                'UORG_EXERCICIO',
                'ORG_EXERCICIO',
                'ORGSUP_EXERCICIO',
                'SITUACAO_VINCULO',
                'REGIME_JURIDICO',
                'JORNADA_DE_TRABALHO']

    CAD_CAT_ID_COLS = [colname + "-ID" for colname in CAD_CAT_COLS]

    REM_COLUMNS = ['ANO', 'MES', 'ID_SERVIDOR_PORTAL',
       'REMUNERAÇÃO BÁSICA BRUTA (R$)', 'REMUNERAÇÃO BÁSICA BRUTA (U$)',
       'ABATE-TETO (R$)', 'ABATE-TETO (U$)', 'GRATIFICAÇÃO NATALINA (R$)',
       'GRATIFICAÇÃO NATALINA (U$)',
       'ABATE-TETO DA GRATIFICAÇÃO NATALINA (R$)',
       'ABATE-TETO DA GRATIFICAÇÃO NATALINA (U$)', 'FÉRIAS (R$)',
       'FÉRIAS (U$)', 'OUTRAS REMUNERAÇÕES EVENTUAIS (R$)',
       'OUTRAS REMUNERAÇÕES EVENTUAIS (U$)', 'IRRF (R$)', 'IRRF (U$)',
       'PSS/RPGS (R$)', 'PSS/RPGS (U$)', 'PENSÃO MILITAR (R$)',
       'PENSÃO MILITAR (U$)', 'FUNDO DE SAÚDE (R$)', 'FUNDO DE SAÚDE (U$)',
       'DEMAIS DEDUÇÕES (R$)', 'DEMAIS DEDUÇÕES (U$)',
       'REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)',
       'REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (U$)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)(*)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (U$)(*)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)(*)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (U$)(*)',
       'TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)',
       'TOTAL DE VERBAS INDENIZATÓRIAS (U$)(*)']

    REM_READ_CFG = {
        "sep": "\t",
        "encoding": "iso-8859-1", 
        "decimal": ",",
        "usecols": REM_COLUMNS,
        "converters": {column_name:STRIP for column_name in REM_COLUMNS}
    }

    OBS_COLUMNS = ['ID_SERVIDOR_PORTAL', 'OBSERVACAO']

    OBS_READ_CFG = {
        "sep": "\t",
        "encoding": "iso-8859-1", 
        "decimal": ",",
        "usecols": OBS_COLUMNS,
        "converters": {column_name:STRIP for column_name in OBS_COLUMNS}
    }

    OBS_CAT_COLS = ['OBSERVACAO']

    OBS_CAT_ID_COLS = [colname + "-ID" for colname in OBS_CAT_COLS]


    def __init__(self, cad_include_filter):

        self.logger = logging.getLogger("RemuneracaoFederalExtractor")
        
        self.cad_include_filter = cad_include_filter
        self.files = [filename for filename in os.listdir(".") if filename.endswith(".zip")]

        self.df = None
        self.encoders = {}


    def ler_zip_csv(self, filepath):

        self.logger.info('>Processando arquivo {}'.format(filepath))
        
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            
            files_in_zip = {filename.split("_")[1]:filename for filename in zip_ref.namelist()}

            with zip_ref.open(files_in_zip['Cadastro.csv']) as zip_file:

                first_line = str(zip_file.readline())

                columns = [column.replace(u"\x00", "") for column in first_line.replace(r"\n", "")\
                                                                               .replace(r"\r", "")\
                                                                               .replace(r"b'", "").split(r"\t")]

                converters = {i:STRIP for i in range(len(columns))}

                iter_csv = pd.read_csv(zip_file, converters=converters, **RemuneracaoFederalExtractor.CAD_READ_CFG)

                cad_df = pd.concat(list(iter_csv))
                cad_df.columns = columns
                cad_df = cad_df[RemuneracaoFederalExtractor.CAD_COLUMNS]
                cad_df = cad_df[self.cad_include_filter(cad_df)]

            with zip_ref.open(files_in_zip['Remuneracao.csv']) as zip_file:
        
                rem_df = pd.read_csv(zip_file, **RemuneracaoFederalExtractor.REM_READ_CFG)

            with zip_ref.open(files_in_zip['Observacoes.csv']) as zip_file:
        
                obs_df = pd.read_csv(zip_file, **RemuneracaoFederalExtractor.OBS_READ_CFG)

            df = pd.merge(cad_df, rem_df, left_on="Id_SERVIDOR_PORTAL", right_on="ID_SERVIDOR_PORTAL")
            df = pd.merge(df, obs_df, left_on="ID_SERVIDOR_PORTAL", right_on="ID_SERVIDOR_PORTAL", how="left")

        return df


    def extract_data(self):

        self.logger.info("EXTRAINDO OS DADOS")

        dfs = [self.ler_zip_csv(zip_file) for zip_file in self.files]

        self.df = pd.concat(dfs, ignore_index=True).fillna("NÃO-ESPECIFICADO")

    def make_encoders(self):

        for (colname, id_colname) in zip(RemuneracaoFederalExtractor.CAD_CAT_COLS, RemuneracaoFederalExtractor.CAD_CAT_ID_COLS):

            self.logger.info(">Normalizando coluna {}".format(colname))

            encoder = LabelEncoder().fit(self.df[colname].unique())
        
            encoder_df = pd.DataFrame(encoder.classes_, columns=[colname])
            encoder_df.index.name = id_colname
            encoder_df.to_csv(colname + ".csv", encoding='utf-8')
            
            self.encoders[colname] = encoder

        for (colname, id_colname) in zip(RemuneracaoFederalExtractor.OBS_CAT_COLS, RemuneracaoFederalExtractor.OBS_CAT_ID_COLS):

            self.logger.info(">Normalizando coluna {}".format(colname))

            encoder = LabelEncoder().fit(self.df[colname].unique())
        
            encoder_df = pd.DataFrame(encoder.classes_, columns=[colname])
            encoder_df.index.name = id_colname
            encoder_df.to_csv(colname + ".csv", encoding='utf-8')
            
            self.encoders[colname] = encoder


    def normalize(self):

        self.logger.info("NORMALIZANDO COLUNAS")

        self.make_encoders()

        self.df[RemuneracaoFederalExtractor.CAD_CAT_ID_COLS] = self.df[RemuneracaoFederalExtractor.CAD_CAT_COLS]\
        .apply(lambda col: self.encoders[col.name].transform(col))
        self.df.drop(RemuneracaoFederalExtractor.CAD_CAT_COLS, axis=1, inplace=True)

        self.df[RemuneracaoFederalExtractor.OBS_CAT_ID_COLS] = self.df[RemuneracaoFederalExtractor.OBS_CAT_COLS]\
        .apply(lambda col: self.encoders[col.name].transform(col))
        self.df.drop(RemuneracaoFederalExtractor.OBS_CAT_COLS, axis=1, inplace=True)


    def save_df(self):

        self.logger.info("SALVANDO O DATASET FINAL")

        self.df.to_csv("dataset.csv", index=False, encoding='utf-8')


    def processar(self):

        self.logger.info('CRIANDO DATASET FINAL')

        self.extract_data()
        self.normalize()
        self.save_df()


if __name__ == "__main__":

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    only_UFC = lambda df: df["ORG_LOTACAO"] == "UNIVERSIDADE FEDERAL DO CEARA"

    gde = RemuneracaoFederalExtractor(cad_include_filter = only_UFC)

    gde.processar()
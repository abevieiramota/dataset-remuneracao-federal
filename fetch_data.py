from urllib import request
import itertools
import logging

meses_2017 = itertools.product([2017], range(1, 13))
meses_2018 = itertools.product([2018], range(1, 7))

logger = logging.getLogger("FetchingRemuneracaoFederal")

URL = "http://portaldatransparencia.gov.br/download-de-dados/servidores/{}{:02d}_Servidores"
FILENAME = "remuneracao_federal_civil_{:02d}_{}.zip"

if __name__ == '__main__':

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    logger.info("BAIXANDO ARQUIVOS DE REMUNERAÇÃO DE SERVIDORES CIVIS FEDERAIS")
    
    for (ano, mes) in itertools.chain(meses_2017, meses_2018):

        url = URL.format(ano, mes)
        logger.info(">Baixando arquivo de %d/%d\nURL: %s", mes, ano, url)

        filename = FILENAME.format(mes, ano)

        request.urlretrieve(url, filename)

from urllib import request
import itertools
import logging

ANOS = range(2013, 2018)
MESES = range(1, 13)
logger = logging.getLogger("FetchingRemuneracaoFederal")

URL = "http://arquivos.portaldatransparencia.gov.br/downloads.asp?a={}&m={:02d}&d=C&consulta=Servidores"
FILENAME = "remuneracao_federal_civil_{:02d}_{}.zip"

if __name__ == '__main__':

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    logger.info("BAIXANDO ARQUIVOS DE REMUNERAÇÃO DE SERVIDORES CIVIS FEDERAIS")
    
    for (ano, mes) in itertools.product(ANOS, MESES):

        logger.info(">Baixando arquivo de {:02d}/{}".format(mes, ano))

        url = URL.format(ano, mes)
        filename = FILENAME.format(mes, ano)

        request.urlretrieve(url, filename)
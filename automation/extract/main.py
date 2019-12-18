import argparse #to use the command line
import csv 
import datetime
import logging #other form to show informatión in console, similar to print
#configure us login module 
logging.basicConfig(level=logging.INFO)

import news_page_objects as news
#regular expression module
import re

#para manejar los errores
from requests.exceptions import HTTPError, ContentDecodingError #TODO averiguar estas exceptions
from urllib3.exceptions import MaxRetryError, DecodeError
from common import config

#give name file --> __name__ 
logger = logging.getLogger(__name__)

#regular expression to verify links
# ^h --> simbolo con el que empieza
# s? --> is optional
# .+ --> uno o más letras
# $ --> terminal el patron
is_well_formed_link = re.compile(r'^https?://.+/.+$') # https://example.com/hello
is_root_path = re.compile(r'^/.+$') # /some-text

#news_site_uid para este caso es eluniversal ó elpais
def _news_scraper(news_site_uid):
    counter = 0
    #we want acces to news_sites, we pass id select for users
    #and requests(solicitamos) url of news_site_uid
    host = config()['news_sites'][news_site_uid]['url']
    
    #print message with url requested
    logging.info('Beggining scraper for {}'.format(host))
    homepage = news.HomePage(news_site_uid, host)

    articles = []
    for link in homepage.article_links:
        #print(link)
        #dado que no todos los links no vienen con la estructura correcta
        #entonces _fetch... nos devolvera un link correcto al cual podamso acceder
        article = _fetch_article(news_site_uid, host, link)

        #si hay un link con body entonces
        if article:
            logger.info('Article fetched!!')
            articles.append(article)
            #print('titulo loks: {}'.format(article.title))
            counter +=1
            print(counter)
            if counter >= 5:
                break
            
    _save_articles(news_site_uid, articles)

    
    #article numbers got
    #print(len(article))

def _save_articles(news_site_uid, articles):
    #para conocer el dia de hoy
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    #file name -- type csv
    out_file_name = '{news_site_uid}_{datetime}_articles.csv'.format(
        news_site_uid = news_site_uid,
        datetime= now)

    #headers del csv
    #lambda es una función inline
    #se va a filtrar por las propiedades que no empiecen por "_"
    # dir(articles[0]) se le pasa el 1er articulo
    #el filtro devuelve una iterador pro eso se uda list
    csv_headers = list(filter(lambda property: not property.startswith('_'), dir(articles[0]))
    )

    with open(out_file_name, mode = 'w+', encoding = "utf-8") as f:
        counter = 0
        writer = csv.writer(f)
        writer.writerow(csv_headers)

        for article in articles:
            row = [str(getattr(article, prop)) for prop in csv_headers]
            writer.writerow(row)
            counter +=1
            print(counter)
            
def _fetch_article(news_site_uid, host, link):
    logger.info('Start fetching article at {}'.format(link))

    article = None
    try:
        article = news.ArticlePage(news_site_uid, _build_link(host,link))
    #HTTPError sucede cuando el recurso web no existe, 
    #MaxRetryError elimina la posibilidad que se vaya al infinito tratando de
    #encontrar la pag
    #exc_info = False -->para que no muestre el error
    except (HTTPError, MaxRetryError, DecodeError, ContentDecodingError) as e:
        logger.warning('Error while fetching the article', exc_info = False)

    #si existe el articulo y no tiene body
    if article and not article.body:
        logger.warning('Invalid article. There is no body')
        return None

    return article        

#this function return a correct liks
def _build_link(host, link):

    #si la funcion esta completa, entonces devuelva el link
    if is_well_formed_link.match(link):
        return link
    #si empezo con / entonces concatene
    elif is_root_path.match(link):
        return '{}{}'.format(host,link)
    #si el link no empieza con diagonal estructuramos el link
    else:
        return '{host}/{uri}'.format(host=host, uri=link)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    #access to keys of the map
    #return a iter and for this we used the list
    news_site_choices = list(config()['news_sites'].keys())

    #difference betwen arguments and options is tha first are necessaries
    #add arguments
    parser.add_argument('news_site',
        #help is necessary to use the program or command line
            help='The news site that you want to scrape',
            type= str,
            #choices in this case are eluniversal or elpais
            choices= news_site_choices)

    # as by now we have the arguments
    #ask to parser, that "parsee" and return an object 
    #with write python main.py --help argparse generate a command line with info
    args = parser.parse_args()
    #send site option select for the user
    _news_scraper(args.news_site)


import argparse
import logging
logging.basicConfig(level = logging.INFO)

import pandas as pd
from article import Article
from base import Base, engine, Session

logger = logging.getLogger(__name__)

def main(filename):
    #permite crear el schema en el motor de base de datos
    #en otras palabras, crear la tabla
    Base.metadata.create_all(engine)
    #se inicia la sesión, esta sesion tiene metodos(add, delete, commit) para interactuar con la base de datos 
    session = Session()
    articles = pd.read_csv(filename)

    #iterrrows(de pandas) permite generar un loop en cada una de las filas del dataframe 
    for index, row in articles.iterrows():
        logger.info('Loading article uid {} info DB'.format(row['uid']))
        #se pasan los datos del dataframe a un objeto article
        article = Article(row['uid'],
                            row['body'],
                            row['host'],
                            row['Newspaper_uid'],
                            row['n_tokens_body'],
                            row['n_tokens_title'],
                            row['title'],
                            row['url'])

        #add article into database
        session.add(article)

    #el metodo commit permite aplicar los cambios en la base de datos
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)

    args = parser.parse_args()

    main(args.filename)

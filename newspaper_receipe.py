
#for get arguments
import argparse
import hashlib
#show to user messages
import logging 
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import nltk
from nltk.corpus import stopwords

#nltk.download('punkt')  //descargar si es la primera vez que se usa
#nltk.download('stopwords')
import pandas as pd

#reference to logging is for know where are executing our code
logger = logging.getLogger(__name__)

#especified lenguaje of stopwords
stop_words =set(stopwords.words('spanish'))

def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_from_body(df)
    df = _tokenize_column(df, 'title') #get count importan words in title or body(only change in function)
    df = _tokenize_column(df, 'body') #get count importan words in title or body(only change in function)

    return df

def _read_data(filename):
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
    logger.info('Extracting newpaper uid')
    #split with '_' because the file name is for example eluniversal_2019_11_14_articles
    newspaper_uid = filename.split('_')[0]

    logger.info('Newpaper uid detected: {}'.format(newspaper_uid))
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uid column with {}'.format(newspaper_uid))
    df['Newspaper_uid'] = newspaper_uid

    return df

def _extract_host(df):
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    
    return df

def _fill_missing_titles(df):
    logger.info('Filling missing titles')
    #select titles that no exist 
    missing_titles_mask = df['title'].isna()

    #get url and apply regular expression for get the last part of url (https://oaxaca.eluniversal.com.mx/salud/como-revertir-y-prevenir la diabetes)
    missing_titles = (df[missing_titles_mask]['url']
                    .str.extract(r'(?P<missing_titles>[^/]+)$')
                    .applymap(lambda title: title.replace('-',' '))
                    )
    
    #print(missing_titles)

    #titles don´t finding are replace with whole list of missing_titles, tag choose in regular expression
    #remember, loc is an attribute access a group of row and columns 
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return df

#generate ids with hashlib.md5 function and utf encode
#axis = 1  --> rows
#axis = 0 --> cols
#all coments are in jupyter notebook --> datawrangling.ipynb
def _generate_uids_for_rows(df):
    logger.info('Generating uids for each row')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())),axis= 1)
            .apply(lambda hash_object: hash_object.hexdigest()) #hexadecimal representation
            )
    df['uid'] = uids

    #uids as index
    return df.set_index('uid')

#remover '\n' '\r' del body
def _remove_new_lines_from_body(df):
    logger.info('Remove new lines from body')

    stripped_dody = (df
                    .apply(lambda row: row['body'], axis= 1)
                    .apply(lambda body: list(body)) # se crea una lista con todo el body
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\n',' '),letters))) #se remplaza \n por ' '
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\r',' '),letters)))
                    .apply(lambda letters: ''.join(letters))
                    )
    df['body'] = stripped_dody #body es remplazado por stripped_body

    return df

def _tokenize_column(df, column_name):
    logger.info('Enrichment title')
    enrichment_title = (df
                        .dropna()
                        .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
                        .apply(lambda tokens: list(filter(lambda token: token.isalpha(),tokens)))
                        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                        .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
                        .apply(lambda valid_word_list: len(valid_word_list))
                        )
    newcolname = 'n_tokens_' + column_name
    df[newcolname] = enrichment_title

    return df 
                    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #filename save input argument
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)
    args = parser.parse_args()
    
    df =  main(args.filename)
    print(df)




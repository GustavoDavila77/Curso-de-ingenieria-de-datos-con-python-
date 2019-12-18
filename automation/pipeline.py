#este es el punto de acceso y el programa
#encargado de realizar la automatización
import logging
logging.basicConfig(level= logging.INFO)
#es parte de la libreria estandar de python
#permite manipular archivos de terminal
import subprocess
#modulo que ofrece la funcionalidad de hacer uso del sistema operativo(ej:. leer y escribir archivos)
import os
#este modulo permite ejecutar operaciones de archivo de alto nivel(copiar, archivar, mover)
import shutil

logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']

def main():
    try:
        _extract()
        _transform()
        _load()
        logger.info('ETL process finished')
    except FileNotFoundError as err:
        logger.warning(str(err))
    except Exception as e:
        logger.warning('Process Error')
        logger.warning(str(e))


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        #se corre un subproceso, se pasa los argumentos para correr el main del extract
        #cwd: current working directory  --> donde se encuentra el programa
        subprocess.run(['python', 'main.py', news_site_uid], cwd= './extract')
        path = '.\\extract'
        file = _search_file(path, news_site_uid)
        #se mueve el archivo generado a la carpeta de transform
        _move_file(path + '\\' + file, '.\\transform\\' + file)


def _transform():
    logger.info('Starting tranform process')
    for news_site_uid in news_sites_uids:
        #dirty_data_filename es el archivo sin limpiar
        dirty_data_filename = _search_file('.\\transform', news_site_uid)
        clean_data_filename = f'clean_{dirty_data_filename}'
        #se corre el programa main para transformar/limpiar el dataset
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd = './transform')
        _remove_file('.\\transform', dirty_data_filename)
        _move_file('.\\transform\\' + clean_data_filename, '.\\load\\' + clean_data_filename)
        #'rm' remove 
        #subprocess.run(['rm', dirty_data_filename], cwd = './transform')
        #subprocess.run(['mv', clean_data_filename, '../load/{}.csv'.format(news_site_uid)],
        #cwd = './transform')

def _load():
    logger.info('starting load process')
    for news_site_uid in news_sites_uids:
        clean_data_filename = _search_file('.\\load', news_site_uid)
        #clean_data_filename = '{}.csv'.format(news_site_uid)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd= './load')
        _remove_file('.\\load', clean_data_filename)

def _remove_file(path, file):
    logger.info(f'Removing file {file}')
    os.remove(f'{path}\\{file}')

def _search_file(path, file_match):
    logger.info('Searching file')

    #se optinen las rutas apartir de un path(dirección)
    #os.walk genera un arbol de archivos en un directorio 
    for rutas in list(os.walk(path))[0]:
        if len(rutas) > 1:
            #por cada archivo en las rutas se pregunta si se encontro el archivo 
            for file in rutas:
                if file_match in file:
                    return file
    return None

def _move_file(origen, destino):
    logger.info('Moving file')
    shutil.move(origen, destino)

if __name__ == '__main__':
    main()
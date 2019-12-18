#this program load the configuration
#conda install pyyaml for install yaml
import yaml

#se cachea(almacena temporalmente) la configuración, ya esta 
#se lee desde disco y por velocidad no se quiere
#cargar desde disco cada vez que se requiera la config
__config = None

def config():
    global __config
    if not __config:
        #open file config.yaml 
        with open('config.yaml',mode='r') as f:
            __config = yaml.load(f, Loader=yaml.FullLoader)

    return __config


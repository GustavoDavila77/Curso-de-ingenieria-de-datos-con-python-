#sqlalchemy es una libreria de python enfocada a interactuar con 
#bases de datos relacionales
from sqlalchemy import create_engine

#permite tener acceso a las funcionalidades de ORM(object relational maper) de sqlalchemy
#el cual permite trabajar  con objetos de python en ves de consultas de sql 
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

#se declara el motor, apuntando al path donde estara ubicado(en este caso esta en el mismo)
# y con el nombre que tendra
engine = create_engine('sqlite:///newspaper.db')

#se genera el objeto Session, se le pasa el motor
Session = sessionmaker(bind=engine)

#se declara la clase base de la cual se van a extender(heredar) todos nuestros modelos
Base = declarative_base()
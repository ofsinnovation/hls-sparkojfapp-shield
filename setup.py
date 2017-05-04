from setuptools import setup, find_packages
from setuptools.command.develop import develop as DevelopCommand


class Develop(DevelopCommand):
    """ Customized setuptools install command which uses pip. """

    def run(self, *args, **kwargs):
        import pip
        pip.main(['install', '.'])
        DevelopCommand.run(self)

setup(
    name='ihealthdata',
    version='1.0.1',
    packages=['ihealthdata',  'ihealthdata.utils', 'ihealthdata.models',
              'ihealthdata.exceptions', 'ihealthdata.helper',
              'ihealthdata.persistence','ihealthdata.consumer','ihealthdata.messaging'],
    url='www.objectfrontier.com',
    license='Limited',
    author='ganapathys',
    author_email='ganapathy.subramani@objectfrontier.com',
    description='These are the microservices to be created for the Frontier Health Platform.',
    install_requires=['configparser==3.5.0','nupic==0.5.7','DBUtils==1.1','kafka-python==1.3.1','sqlalchemy','psycopg2',
                      'cryptography','pandas==0.19.1','numpy','scipy==0.18.1']
    )

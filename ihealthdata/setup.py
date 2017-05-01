from distutils.core import setup

setup(
    name='frontier-health-p-data',
    version='1.0.1',
    packages=['ihealthdata',  'ihealthdata.utils', 'ihealthdata.models',
              'ihealthdata.exceptions', 'ihealthdata.helper',
              'ihealthdata.persistence','ihealthdata.consumer','ihealthdata.messaging'],
    url='www.objectfrontier.com',
    license='Limited',
    author='ganapathys',
    author_email='ganapathy.subramanian@object-frontier.com',
    description='These are the microservices to be created for the Frontier Health Platform.'
    )

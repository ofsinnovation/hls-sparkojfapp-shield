import pip


def install(package):
    pip.main(['install', package])


if __name__ == '__main__':
    install('numpy')
    install('configparser==3.5.0')
    install('nupic==0.5.7')
    install('DBUtils==1.1')
    install('kafka-python==1.3.1')
    install('sqlalchemy')
    install('psycopg2')
    install('cryptography')
    install('pandas==0.19.1')
    install('scipy==0.18.1')

    import ihealthdata.consumer.ihealth_anomaly_detection as ad
    c = ad.Consumer()
    c.trigger_stream()
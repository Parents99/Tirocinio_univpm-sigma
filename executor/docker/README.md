Nel docker-compose viene definita la variabile d'ambiente PARTNER_VOLUME_PATH che viene letta a runtime durante l'esecuzione, e che rappresenta il path del volume in cui risiedono gli script del partner e i suoi ambienti virtuali.

Viene mappato inoltre un volume che è il volume specifico del partner in cui sono presenti i suoi script e i suoi ambienti virtuali.

Il file requirements.txt definisce i moduli python necessari al Web Service Flask.
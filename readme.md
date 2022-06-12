Pour mettre en place notre application, nous avons les dépendances:
- Installation et configuration de Spark
- Installer et configurer Anaconda 
- Créer un environnement virtual python et l'activé
- Installer pyspark
- Installer et configurer jupyter notebook
- Configurer la variable d'envirronnement PYSPARK_PYTHON = Chemin vers le python de l'environnement virtual python utilisé.


Pour exécuter avec spark, il faut d'abord démarrer le master node  avec
spark-class org.apache.spark.deploy.master.Master,

Et exécuter avec la commande suivante :
 spark-submit --master  adress-du-master  Chemin-projet\main.py 

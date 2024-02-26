
<html>
<head>
 
 
</head>
<body>

<img src="https://www.ffmkr.org/_upload/cache/ressources/logos/logo-doctolib.svg_298_139.jpg" alt="Doctolib Logo" class="center">

</body>
</html>

# Documentation d'exploitation pour le projet Doctolib Test Case Pipeline

## Introduction

Ce document fournit un guide d'exploitation pour le projet Doctolib Test Case Pipeline. Le projet utilise Docker pour gérer les conteneurs et Airflow pour orchestrer les tâches ETL. PostgreSQL est utilisé comme base de données pour stocker les données transformées.

## Prérequis

- Docker doit être installé sur la machine hôte.
- Git doit être installé sur la machine hôte pour cloner le référentiel du projet.

## Installation

1. Clonez le référentiel du projet depuis GitHub :

   ```bash
   git clone https://github.com/antoinersw/doctolib_test_case_pipeline.git
   ```

2. Accédez au répertoire du projet :

   ```bash
   cd doctolib_test_case_pipeline
   ```

3. Lancez les conteneurs Docker en utilisant Docker Compose :

   ```bash
   docker-compose up
   ```

   Cette commande va construire les images Docker nécessaires et démarrer les conteneurs pour Airflow, PostgreSQL et d'autres services.

## Initialisation

1. Accédez au conteneur Airflow :

   ```bash
   docker exec -it doctolib_test_case_pipeline-webserver-1 /bin/bash
   ```

2. Importez les variables Airflow à partir du fichier `variables.json` :

   ```bash
   airflow variables import  variables/variables.json
   ```

3. Accédez au conteneur PostgreSQL :

   ```bash
   docker exec -it doctolib_test_case_pipeline-postgres-1 /bin/bash
   ```

4. Créez la base de données DWH :

   ```bash
   createdb -U airflow DWH
   ```

## Utilisation

1. Accédez à l'interface Web d'Airflow en ouvrant un navigateur et en visitant `http://localhost:8080`.

2. Connectez-vous à Airflow en utilisant les informations d'identification par défaut (utilisateur : `airflow`, mot de passe : `airflow`).

3. Activer les DAG depuis l'UI pour lancer le pipeline ETL => ils se lanceront automatiquement car la start_date est réglée sur J-1

## Maintenance

1. Pour arrêter les conteneurs Docker, utilisez la commande suivante dans le répertoire du projet :

   ```bash
   docker-compose down
   ```

2. Pour supprimer les conteneurs, les images et les volumes Docker, utilisez la commande suivante :

   ```bash
   docker-compose down --volumes --rmi all
   ```
 
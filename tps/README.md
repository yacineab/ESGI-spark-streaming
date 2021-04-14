# Spark Streaming
## Structured Streaming

### Creating RDD from source
Nous travaillons avec les données `activity-data`  

#### Heterogeneity Human Activity Recognition Dataset
The data consists of smartphones and smartwatch sensor reading from a variety of devices

##### Lire une version static du Dataset
1. Créer un dataframe pour lire votre jeu de données
 - NB: le activityData est un répertoire qui contient plusieurs fichiers json. Spark se charge de lire tous les fichiers contenu dans ce répertoire comme un seul DataFrame 
1. créer une variable qui contient le schema du Dataframe Static, Utiliser la fonction ``` .schema ```  .ce Schema sera utilisé pour l'inférer au stream par la suite
1. faire un select ``` distinct ``` sur la colone ``` gt ``` et ensuite un ```show() ```
##### The ``` gt ``` column: montre l'activité du user à un moment donné (i.e: sit, stand,walk, bike, stairsup, stairsdown) 

### Straming du DataFrame
1. Pour pouvoir inférer le schema définie plus haut ```dataSchema``` à notre stream:
Setté la conf ``` spark.sql.streaming.schemaInference ```à ``` true ```
1. Pour créer un strcutured stream ```streaming``` qui va utiliser comme input source les fichiers json du répertoire ```activity-data```
    - lire le répertoire de donnée comme un dataframe de stream 
    - Utiliser ```.schema``` pour definir le schema de votre stream
    - utiliser l'option ```option("maxFilesPerTrigger",1)``` pour dire à spark que vous allez processer dans votre streaming 1 seul fichier à la fois

### Streaming Transformation
1. à partir du Dataframe ```streaming``` créer un dataframe ```activityCounts``` qui calcule le nombre de personne effectuant chaque activité.

Pour rappelle les données par type d'activité sont groupé dans la colonne  ```gt```

### Streaming Action
La transformation définie plus haut est lazy, il faut spécifier une ```action```pour commencé la requete

Pour cela on doit spécifier:

  - le ```format```: output destination ou l'output sink pour cette query. Pour notre exemple on va travailler avec le sink ```memory```
  - ```queryName("activity_count")``` Pour donner un nom à la table de sortie
  - ```outputMode``` : complete pour notre exemple, ce mode rewrite toutes les clés et leurs count à chaque trigger ( un trigger est defini à un fichier plus haut, par )

1. Démarrer votre stream pour ecrire chaque stream dans le sink ```memory```
 
  
 ### Check for active streams

Spark liste les streams actifs dans l'objet ```streams```du sparkSession ```spark```
1. Afficher les active stream dans le sparkSession 

### Requete l'active stream
Le stream est envoyé en sink en mémoire où il est considérer comme une table sous le nom ```activity_counts```.

On peut donc la requeter:
 - On peut faire une simple loop sur les streams en sinks et show le resultat (on a créé un fichier par stream, on doit avoir autant de stream en sink que de fichier en source) 
 - attendre 1 seconde entre chaque resultat.
 
1. Completer cette fonction pour faire cela: 

```java
for (i<- 1 to 15) {
 spark.sql(" SELECT... ").show()
 Thread.sleep(1000)
}
```


### Transformations on Streams

1. à partir du dataframe stream précédent créer un nouveaux stream en application ces transformation  :
  - ajouter une colonne "stairs" qui est à true si la colonne ```gt``` contient le mot ```stairs```
  - filtrer sur la nouvelle colonne ```stairs``` (sa valeur est à true)
  - selectionner les colonne suivante: ```"gt","model","arrival_time","creation_time","stairs" ```
1. Ecrire le nouveau stream dans le sink ```memory``` et démarrer le stream


#### Requeter un stream
1. Requeter comme précedement le stream ```simple_transformation```
en utilisant:
```java
for (i<- 1 to 15) {
spark.sql(" SELECT... ").show()
Thread.sleep(1000)
}
```

#PARTIE 2

##### Cette partie est orienté Recherche et Développement
- Cette partie doit être ecrite en mode batch dans un premier temps, la solution streaming peut etre envisagée après validation de la solution
- Plusieurs solutions sont possibles 
- 2 points de bonus au prochain CC seront attribué pour les solutions qui réponde à la problèmatique en mode batch (ou soutenance si vous avez plus de 18/20 au CC)

### Données

Nous analysons des données de chatbot disponible dans le répetoire data/chatbot-data.

Les données sont au format Json et ressemble à ceci :

```json
{
    "name": "Roslyn Brown",
    "gender": "female",
    "age": 40,
    "eyeColor": "green",
    "company": "SINGAVERA",
    "isActive": false,
    "registered": "2015-04-24T06:51:05 -02:00",
    "input1": "Veniam Lorem cillum dolor mollit Lorem non duis nostrud elit veniam anim.",
    "output1": "Adipisicing et nulla cillum duis labore magna proident ut enim eu magna aliqua irure.",
    "input2": ... ,
    "output2": ... ,
  }
```

##### Les données JSON contienne deux parties:
- **Partie Fixe:** ces champs sont présents dans tous les fichiers
   - **Les champs:** "name","gender","age","eyeColor","company","isActive","registered"
- **Partie variables:** ces champs sont présents ou pas en fonction de la conversation sur le chatBot
  - **Les champs:** input1, output1, input2,output2 ...


### Porblématique
##### L'objectif est d'insérer ces données dans une table Hive - pour notre cas ca peut être un simple fichier csv en sortie
- Le nombre de champs (colonnes) peut varier considérabelement d'un fichier JSON à un autre
- La table Hive doit être crée au prélable, donc on doit connaitre au préalable le nombre de colonnes 
- l'objectif est donc de trouver une solution pour ingérer ces données de chatbot avec un nombre colonnes variables dans une table hive avec un nombre de colonnes fixes

##### Axe de travail
- La table hive sera simulé par un fichier `CSV` donc en place et lieu d'ecrire le dataframe résultat dans une table Hive, nous allons l'écrire dans un fichier CSV
- Trouver une solution pour insérer les données chatbot décrite en haut dans un fichier csv
- Votre job doit tenir compte des nouvelles données chatbot que nous pouvons recevoir et donc avec un nombre de champs `input/output` différents

##### Remarques
Si vous utiliser la method `spark.read.json("path")` ajouter l'option `option("multiline","true")` pour lire les json mutlitlignes:
`spark.read.option("multiline","true").json("path")`

c'est à chacun de réflechir et proposer la solution qui lui semble la plus pertinante pour résoudre cette problèmatique

# coding=utf-8
# Ce script est extrait du notebook du projet 2 (analyse des donn�es nutritionnelles, s�y r�f�rer pour plus d'infos)

#  import des librairies necessaires
import pandas as pd

products = pd.read_csv("products.csv", sep='\t', low_memory=False)


# fonction renvoyant le taux de remplissage d'une colonne col au sein d'une dataframe df donn�e
def col_filling_rate(df, col):
    return 1 - df[col].isnull().sum() / len(df)


# Un dictionaire (une map) associant chaque colonne avec son taux de remplissage au sein de la df products
rate_by_column = dict((col, col_filling_rate(products, col)) for col in products.columns.values)

# Suppression des colonnes vides � plus de 70%
for (col, rate) in rate_by_column.items():
    if col in products and rate < 0.3:
        del products[col]


# on remarque ci-dessus que 13 colonnes (parmi les 43 restantes) sont � toujours � renseign�es et contiennent des informations � plut�t techniques�
# (code, url , created_datetimes, countries_fr �),
# Parmi ces 13 colonnes, seule la donn�e �countries_fr� est prise en compte plus tard dans cette analyse.
# Par ailleurs, on s'assure que les colonnes potentiellement importantes au vu de l'objectif de notre projet, sont "toujours pr�sentes", celles indiquant la quntit� d'un
# composant (sucre, sel, vitamine c, fer ...) dans 100g du produit.
# Pour une "row" dans products, cette fonction calcule le taux de remplissage des 30 cellules (parmi les 43)  correspondant
# au colonnes autres que les 13  cit�es pr�c�demment

def row_filling_rate(row):
    return 1 - sum(row.isnull()) / (43 - 13)


# Ajout d'une nouvelle colonne row_filling_rate
products['row_filling_rate'] = products.apply(lambda row: row_filling_rate(row), axis=1)

# Suppression de l�ensemble des lignes ayant un row_filling_rate < 0.3
products = products[products['row_filling_rate'] >= 0.3]


# fonctions renvoyant les 3 quartiles pour une colonne donn�e.
def q1(col):
    return products[col].quantile(.25)  # premier quartile


def median(col):
    return products[col].quantile(.5)  # deuxi�me quartile


def q3(col):
    return products[col].quantile(.75)  # troisi�me quartile


def l_inf(col):
    return q1(col) - 1.5 * (q3(col) - q1(col))


def l_sup(col):
    return q3(col) + 1.5 * (q3(col) - q1(col))


# Fonction servant � remplacer les valeur manquantes
def fill_na(df, col, new_value):
    if callable(new_value):
        new_value = new_value(df, col)  # l'id�e est de pouvoir passer soit une valeur directe ou bien
    df[col] = df[col].fillna(new_value)  # une lambda renvoyant une valeur de remplacement


# Cette fonction remplace les outliers par la valeur new_value (ou la valeur renvoy�e par new_value s�il s�agit d�une fonction)
def replace_outliers(df, col, predicate, new_value):
    if callable(new_value):
        new_value = new_value(df, col)
    df.loc[predicate(df[col]), col] = new_value


# Fonction de nettoyage de donn�es
# consid�rer comme "outlier" toute valeur o tel que o > q3 + 1.5(q3 - q1) ou o < q1 - 1.5(q3 - q1)
# remplacer les outliers par la m�diane �tant plus robuste est moins sensible au valeurs extr�mes.
# remplacer les manquants par la m�diane

def clean(df, col):
    inf = l_inf(col)
    sup = l_sup(col)
    me = median(col)
    fill_na(df, col, me)
    replace_outliers(df, col, lambda x: (x < inf) | (x > sup), me)


# nettoyage
for col in ['energy_100g', 'trans-fat_100g', 'cholesterol_100g', 'saturated-fat_100g', 'carbohydrates_100g',
            'sugars_100g', 'fat_100g', 'fiber_100g', 'proteins_100g', 'sodium_100g',
            'salt_100g', 'calcium_100g', 'vitamin-c_100g', 'vitamin-a_100g', 'iron_100g',
            'nutrition-score-fr_100g', 'nutrition-score-uk_100g']:
    clean(products, col)

# nettoyage de la donn�e Pays
# Dans un premier temps, on remarque qu'en "splitant" une entr�e dans la colonne "countries_fr" et grace � une
# expression r�guli�re simple on peut avoir par produit une liste contenant les diffrents pays associ�s (dans un etat brut).

import re

# On recense tous les r�sultats possibles, et on utilise un dictionnaire pour traduire les noms des pays
_fr = dict({
    "FRANCIAORSZAG": "FRANCE",
    "FRANKRIKE": "FRANCE",
    "FRANKREICH": "FRANCE",
    "BOUCHES-DU-RHONE": "FRANCE",
    "BOURGOGNE-AUBE-NOGENT-SUR-SEINE": "FRANCE",
    "AIX-EN-PROVENCE": "FRANCE",
    "FRANKRIJK": "FRANCE",
    "MARSEILLE-6": "FRANCE",
    "????????": "ARABIE SAOUDITE",
    "????????-???????-???????": "�MIRATS ARABES UNIS",
    "AUSTRALIEN": "AUSTRALIE",
    "AZ?RBAYCAN": "AZERBA�DJAN",
    "BELGIE": "BELGIQUE",
    "BELGIEN": "BELGIQUE",
    "DENEMARKEN": "DANEMARK",
    "DEUTSCHLAND": "ALLEMAGNE",
    "DUITSLAND": "ALLEMAGNE",
    "FINAND": "FINLANDE",
    "GUYANA": "GUYANE",
    "ISLAND": "ISLANDE",
    "ITALIAANS": "ITALIE",
    "ITALIEN": "ITALIE",
    "KINA": "KENYA",
    "SCHWEIZ": "SUISSE",
    "SVAJC": "SUISSE",
    "MAGYARORSZAG": "HONGRIE",
    "NEDERLAND": "PAYS-BAS",
    "HOLLANDE": "PAYS-BAS",
    "NOORWEGEN": "NORVEGE",
    "OTHER-JAPON": "JAPON",
    "OTHER-TURQUIE": "TURQUIE",
    "OTHER-??": "JAPON",
    "PORTUGALIA": "PORTUGAL",
    "REPUBLIK-CHINA": "CHINE",
    "REPUBLIQUE-DE-CHINE": "CHINE",
    "SPANIEN": "ESPAGNE",
    "SPANJE": "ESPAGNE",
    "SPANYOLORSZAG": "ESPAGNE",
    "SVERIGE": "SU�DE",
    "ZWITSERLAND": "SU�DE",
    "ZWEDEN": "SU�DE",
    "SZCZECIN": "POLOGNE",
    "TURKIYE": "TURQUIE",
    "CZECH": "R�PUBLIQUE TCH�QUE",
    "TSCHECHIEN": "R�PUBLIQUE TCH�QUE",
    "VEREINIGTES-KONIGREICH": "ROYAUME-UNI",
    "ANGLETERRE": "ROYAUME-UNI",
    "??????": "IRAK",
    "???????-???????": "ROYAUME-UNI",
    "?????-????": "OMAN",
    "????????????????": "AUSTRALIE",
    "?????????????": "ROYAUME-UNI",
    "?????????": "THA�LANDE",
    "THAILAND": "THA�LANDE",
    "??": "JAPON",
    "??": "HONG KONG",
    "?????????": "KAZAKHSTAN",
    "SCOTLAND": "�COSSE",
    "ETATS-UNIS": "�TATS-UNIS",
    "NAN": "INCONNU"
})


# fonction de traduction
def synonym(x):
    return _fr[x] if x in _fr else x


# Fonction servant � nettoyer une valeur lue dans la colonne countries_fr (la fonction utilise un cache interne pour
# ne faire le traitement qu'une seule fois pour des inputs �gaux)
def clean_country(entry):
    if entry in clean_country.cache:  # memoization du r�sultat
        return clean_country.cache[entry]
    result = [synonym(re.sub(".+:", "", country)) for country in str(entry).upper().split(',')]
    clean_country.cache[entry] = result
    return result


clean_country.cache = {}


# Cette fonction construit un dictionnaire dont la cl� est un pays donn� et la value est une liste regroupant l'ensemble des
# nutrition-score-uk_100g des produits associ�s � ce pays. (on tient compte du fait qu'un produit peut correspondre � n pays )

def scores_by_country():
    scores = dict({})
    for (index, row) in products.iterrows():
        for country in clean_country(row['countries_fr']):
            if country not in scores:
                scores[country] = []
            scores[country].append(row['nutrition-score-uk_100g'])
    return scores


scores = scores_by_country();
# Si un pays n'est cit� que peu de fois ( < 30 ) dans l'ensemble de la df products alors il est supprim� de la variable score
scores = dict((country, _scores) for (country, _scores) in scores.items() if len(_scores) >= 30)


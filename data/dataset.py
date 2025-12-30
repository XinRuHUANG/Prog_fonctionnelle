import pandas as pd
import random
from datetime import datetime, timedelta

# Génération de données fictives pour les mesures
stations_extended = ['Station A', 'Station B', 'Station C', 'Station D', 'Station E', 'Station F', 'Station G', 'Station H',
                     'Station I', 'Station J', 'Station K', 'Station L', 'Station M', 'Station N', 'Station O', 'Station P',
                     'Station Q', 'Station R', 'Station S', 'Station T', 'Station U']
lignes_transport_extended = ['Ligne 1', 'Ligne 2', 'Ligne 3']
météo_extended = ['Ensoleille', 'Nuageux', 'Pluvieux', 'Neigeux', 'Brumeux', 'Orageux']
jours_semaine_extended = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']

# Fonction pour ajuster les valeurs en fonction de l'heure de la journée et ajouter l'affluence
def ajuster_pointe(horaire):
    heures, minutes = map(int, horaire.split(":"))
    
    if (7 <= heures < 9) or (17 <= heures < 19):  # Heures de pointe (7-9 et 17-19)
        co2 = random.uniform(350, 500)
        particules_fines = random.uniform(40, 80)
        bruit = random.uniform(60, 90)
        affluence = 'Elevee'
    elif (9 <= heures < 17):  # Heures moyennes (9-16)
        co2 = random.uniform(250, 350)
        particules_fines = random.uniform(20, 40)
        bruit = random.uniform(40, 60)
        affluence = 'Moyenne'
    else:  # Heures creuses (00-6)
        co2 = random.uniform(200, 250)
        particules_fines = random.uniform(10, 20)
        bruit = random.uniform(30, 40)
        affluence = 'Faible'
        
    return co2, particules_fines, bruit, affluence

# Dictionnaire station → lignes (une station peut avoir plusieurs lignes)
station_ligne_mapping = {
    'Station A': ['Ligne 1'],
    'Station B': ['Ligne 1'],
    'Station C': ['Ligne 1'],
    'Station D': ['Ligne 1', 'Ligne 2'],
    'Station E': ['Ligne 1'],
    'Station F': ['Ligne 1'],
    'Station G': ['Ligne 1'],
    'Station H': ['Ligne 1', 'Ligne 3'],
    'Station I': ['Ligne 2'],
    'Station J': ['Ligne 2'],
    'Station K': ['Ligne 2'],
    'Station L': ['Ligne 2'],
    'Station M': ['Ligne 2', 'Ligne 3'],
    'Station N': ['Ligne 2'],
    'Station O': ['Ligne 2'],
    'Station P': ['Ligne 3'],
    'Station Q': ['Ligne 3'],
    'Station R': ['Ligne 3'],
    'Station S': ['Ligne 3'],
    'Station T': ['Ligne 3'],
    'Station U': ['Ligne 3']
}

# Générer des données supplémentaires pour un mois (500 instances)
dates_extended = [datetime.today().date() + timedelta(days=i) for i in range(1000)]
horaires_extended = [f"{random.randint(0, 23)}:{random.randint(0, 59)}" for _ in range(1000)]
co2_extended = []
particules_fines_extended = []
bruit_extended = []
humidite_extended = [random.uniform(40, 80) for _ in range(1000)]  # en pourcentage
stations_data_extended = [random.choice(stations_extended) for _ in range(1000)]

# Appliquer la fonction de mappage station → lignes
lignes_transport_data_extended = [random.choice(station_ligne_mapping[station]) for station in stations_data_extended]

# Autres données aléatoires pour la météo, le jour de la semaine, etc.
météo_data_extended = [random.choice(météo_extended) for _ in range(1000)]
jours_data_extended = [random.choice(jours_semaine_extended) for _ in range(1000)]
semaines_data_extended = [(date.isocalendar()[1]) for date in dates_extended]
affluence_extended = []

# Appliquer l'ajustement des valeurs de pollution et l'affluence en fonction des horaires
for horaire in horaires_extended:
    co2, particules_fines, bruit, affluence = ajuster_pointe(horaire)
    co2_extended.append(co2)
    particules_fines_extended.append(particules_fines)
    bruit_extended.append(bruit)
    affluence_extended.append(affluence)

# Création du DataFrame
data_extended = {
    'Date': dates_extended,
    'Horaire': horaires_extended,
    'CO2 (ppm)': co2_extended,
    'Particules fines (µg/m3)': particules_fines_extended,
    'Bruit (dB)': bruit_extended,
    'Humidite (%)': humidite_extended,
    'Station': stations_data_extended,
    'Ligne de transport': lignes_transport_data_extended,
    'Meteo': météo_data_extended,
    'Jour de la semaine': jours_data_extended,
    'Semaine': semaines_data_extended,
    'Affluence': affluence_extended,
}

df_extended = pd.DataFrame(data_extended)

# Sauvegarder en CSV
csv_file_path = 'pollution_data.csv'
df_extended.to_csv(csv_file_path, index=False)

print(f"Le fichier CSV a été créé et enregistré sous le nom {csv_file_path}")

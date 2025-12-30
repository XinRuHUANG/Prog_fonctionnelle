package model

case class PollutionRecord(
                            Date: String,
                            Horaire: String,
                            `CO2 (ppm)`: Double,
                            `Particules fines (µg/m3)`: Double,
                            `Bruit (dB)`: Double,
                            `Humidite (%)`: Double,
                            Station: String,
                            `Ligne de transport`: String,
                            Meteo: String,
                            `Jour de la semaine`: String,
                            Semaine: Int,
                            Affluence: String
                          )

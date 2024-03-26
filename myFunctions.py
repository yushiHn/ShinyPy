import numpy as np
from numpy import random
from matplotlib import pyplot as plt


import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pandas as pd
import numpy as np
from datetime import datetime, date

import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import Row

from pyspark.sql.functions import *



def load_Spark_Star(a):


    spark = SparkSession.builder.getOrCreate()  

    psdf_administration = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_administration.csv', index_col='KeyConsult')
    psdf_medecins = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_medecins.csv', index_col='KeyMedecin')

    psdf_diagnostics = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_diagnostics.csv', index_col='KeyConsult')
    psdf_treatments = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_treatments.csv', index_col='KeyTreatment')
    psdf_medicaments = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_medicaments.csv', index_col='KeyMedicament')
    psdf_chambres = ps.read_csv('/Users/giletcyp/Desktop/Cours/NF26/Chapitre 4/NF26_AI07_TD4_Correction/NF26_AI07_TD4_Exercices/data_chambres.csv', index_col='KeyChambre')

    sdf_administration = psdf_administration.to_spark(index_col='KeyConsult')
    sdf_medecins = psdf_medecins.to_spark(index_col='KeyMedecin')
    sdf_diagnostics = psdf_diagnostics.to_spark(index_col='KeyConsult')
    sdf_treatments = psdf_treatments.to_spark(index_col='KeyTreatment')
    sdf_medicaments = psdf_medicaments.to_spark(index_col='KeyMedicament')
    sdf_chambres = psdf_chambres.to_spark(index_col='KeyChambre')

    sdf_FAITS_1 = sdf_diagnostics.select('KeyConsult','KeyPatient','KeyMedecin','KeyTreatment').alias('sdf_FAITS_1')
    sdf_FAITS_2 = sdf_FAITS_1.withColumn('KeyDate', sdf_FAITS_1.KeyConsult)
    sdf_FAITS_consults = sdf_FAITS_2.join(sdf_administration, 'KeyConsult').select(sdf_FAITS_2["*"],sdf_administration.KeyChambre)

    sdf_dim_patients = sdf_diagnostics.select('KeyPatient','KeyConsult','NamePatient','FirstNamePatient','NumSecu','Age','Weight','Temperature','Tension','Diabete','Pathology').alias('sdf_dim_patients')

    sdf_dim_date1 = sdf_FAITS_consults.select('KeyDate').alias('sdf_dim_date1')
    sdf_adm = sdf_administration.select('KeyConsult','Date_In','Date_Out').alias('sdf_adm')
    sdf_dim_dates = sdf_dim_date1.alias('sdf_dim_date1').join(sdf_adm.alias('sdf_adm'), sdf_dim_date1.KeyDate==sdf_adm.KeyConsult).drop('KeyConsult')

    sdf_dim_medecins = sdf_medecins.select('*')

    sdf_dim_traitement1 = sdf_treatments.alias('sdf_dim_traitements')
    sdf_dim_traitements = sdf_dim_traitement1.join(sdf_medicaments, sdf_dim_traitement1.KeyMedicament==sdf_medicaments.KeyMedicament).select(sdf_dim_traitement1["*"],sdf_medicaments["NameMedicament"])

    sdf_dim_chambres = sdf_chambres.select('*')

    PathologyQuest = "Pathology75"
    YearQuest = 2023
    MonthQuest = 3

    df_DatesQuest = sdf_dim_dates.filter((year("Date_in")==YearQuest) & (month("Date_in")==MonthQuest)).select(col("KeyDate"))
    #df_DatesQuest = sdf_dim_dates.filter(year("Date_in")==YearQuest).filter(month("Date_in")==MonthQuest).select(col("KeyDate"))
    df_PathologyQuest = sdf_dim_patients.filter(col("Pathology")==PathologyQuest)

    FactDatesQuest = sdf_FAITS_consults.alias("t1").join(df_DatesQuest.alias("t2"),'KeyDate').select(sdf_FAITS_consults["*"])
    PatientsObjectifQuest = df_PathologyQuest.alias("s1").join(FactDatesQuest.alias("s2"),'KeyConsult').select(df_PathologyQuest["*"])
    a = PatientsObjectifQuest.select(avg(col("Age")))

    return np.array(a.toPandas())[0,0]



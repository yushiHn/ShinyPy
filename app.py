# This app is translated from Mastering Shinywidgets
# https://mastering-shiny.org/basic-reactivity.html#reactive-expressions-1
import pip
pip.main(['install','shiny'])
from shiny import App, render, ui
from numpy import random

# Functions we import from myFunctions.py
from myFunctions import load_Spark_Star
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pandas as pd
import numpy as np
from datetime import datetime, date

import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import Row


from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()




app_ui = ui.page_fluid(
    ui.row(
        ui.column(
            2,
            "Pathology",
            ui.input_text("text", "Text input", "Enter Pathology..."),  
            #ui.output_text_verbatim("value"),
        ),
        ui.column(
            3,
            "Période",
            ui.input_date("date_entree", "Date Entrée"),  
            ui.input_date("date_sortie", "Date Sortie"),  
        ),
        ui.column(
            2,
            "Requête ?",
            ui.input_select(  
                "select",  
                "Select an option below:",  
                {"1A": "Quel a été l'âge moyen des patients en question ?", 
                 "1B": "Quel Médicament a été le plus prescrit (en terme de quantité) ?", 
                 "1C": "Combien de chambres ont accueilli... ?"},  
                ),  
            #ui.output_text("value"),
        ),
    ),
    ui.row(
        #ui.column(9, ui.output_plot("hist")),
        ui.column(3, ui.output_text_verbatim("compute_age")),
    ),
)


def server(input, output, session):   

    @output
    @render.text
    def compute_age():
        age = load_Spark_Star(1)
        return f""" AVG age: {age}"""


app = App(app_ui, server)


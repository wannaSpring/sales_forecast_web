import random
from flask import Flask
from flask import render_template, request
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# from pyspark.sql.types import *
import pickle
import pandas as pd

app = Flask(__name__)

# get spark session
# spark = SparkSession.builder \
#     .master("local") \
#     .appName("Sales_Predict") \
#     .getOrCreate()

# load model
model = pickle.load(open('model/rf_model.sav', 'rb'))


# index webpage receives user input for the model
@app.route('/')
@app.route('/index')
def index():
    # render web page
    return render_template('master.html')


# web page that handles user query and displays model results
@app.route('/go')
def go():
    # get parameters from the form
    year = request.args.get('year', 0)
    month = request.args.get('month', 0)
    shop = request.args.get('shop', '')
    item = request.args.get('item', '')
    shop_id = 0
    item_id = 0

    # encode shop_id
    if shop == 'Атриум':
        shop_id = 25
    elif shop == 'МЕГА Теплый Стан':
        shop_id = 28
    elif shop == 'Семеновский':
        shop_id = 31
    elif shop == 'Невский Центр':
        shop_id = 42
    elif shop == 'Якутск Орджоникидзе':
        shop_id = 57

    # encode item_id
    if item == 'ЗОЛОТАЯ КОЛЛЕКЦИЯ м/ф-72':
        item_id = 19
    elif item == 'ОДНАЖДЫ В КИТАЕ-2':
        item_id = 20
    elif item == 'СЕВЕР И ЮГ Ч.2':
        item_id = 23
    elif item == '007 Legends [PС, Jewel, русская версия]':
        item_id = 28
    elif item == 'КООРДИНАТЫ «СКАЙФОЛЛ»':
        item_id = 30
    elif item == '10 ЛЕТ СПУСТЯ (регион)':
        item_id = 37
    elif item == '100 Best classical melodies (mp3-CD) (Digipack)':
        item_id = 40
    elif item == '100 любимых маленьких сказок (mp3-CD) (Jewel)':
        item_id = 55
    elif item == '11 ДРУЗЕЙ ОУШЕНА WB (BD)':
        item_id = 71

    df = pd.DataFrame()
    # get spark context
    # sc = SparkContext.getOrCreate()
    #
    # # create spark dataframe to predict customer churn using the model
    # df = sc.parallelize([[gender, days_registered, last_state, avgSongs, last_level, Thumbsup_proportion,
    #                       num_add_friend, avgrolladverts]]). \
    #     toDF(["gender", "days_registered", "last_state", "avg_songs_per_day", "last_level", "Thumbsup_proportion",
    #           "num_add_friend", "avg_roll_adv_per_day"])
    #
    # # set correct data types
    # df = df.withColumn("days_registered", df["days_registered"].cast(IntegerType()))
    # df = df.withColumn("avg_songs_per_day", df["avg_songs_per_day"].cast(DoubleType()))
    # df = df.withColumn("Thumbsup_proportion", df["Thumbsup_proportion"].cast(DoubleType()))
    # df = df.withColumn("num_add_friend", df["num_add_friend"].cast(IntegerType()))
    # df = df.withColumn("avg_roll_adv_per_day", df["avg_roll_adv_per_day"].cast(DoubleType()))

    # predict using the model
    # pred = rf_model.predict(df)

    # if pred.count() == 0:
    #     # if model failed to predict churn then return -1
    #     prediction = -1
    # else:
    #     # get prediction (1 = churn, 0 = stay)
    #     prediction = pred.select(pred.prediction).collect()[0][0]

    prediction = random.randint(0,20)

    # print out prediction to the app console
    print("Prediction for the customer is {prediction}.".format(prediction=prediction))

    # render the go.html passing prediction resuls
    return render_template(
        'go.html',
        result=prediction
    )


def main():
    app.run(host='0.0.0.0', port=3001, debug=True)


if __name__ == '__main__':
    main()

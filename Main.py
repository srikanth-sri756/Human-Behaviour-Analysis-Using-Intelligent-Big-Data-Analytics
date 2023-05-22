from tkinter import messagebox
from tkinter import *
from tkinter import simpledialog
import tkinter
import matplotlib.pyplot as plt
import numpy as np
from tkinter import ttk
from tkinter import filedialog
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import time

main = Tk()
main.title("Human Behavior Analysis Using Intelligent Big Data Analytics")
main.geometry("1300x1200")

global filename
global execution_time
global df_frame

def initSpark():
    global df_frame
    spark = SparkSession.builder.master("local[1]").appName("HDFS").getOrCreate()
    sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("HDFS"))
    logs = sparkcont.setLogLevel("ERROR")
    dataset = spark.read.format("csv").option("header",True).load("Dataset/dailymotion.csv")
    frame = dataset[['likes','dislikes','view_count','comment_count','description']]
    frame = frame.toPandas()
    df_frame = frame.iloc[:,0:5].values
    text.delete('1.0', END)
    text.insert(END,"SPARK initialization Done!")
    
def uploadDataset():    
    global filename
    global dataset
    text.delete('1.0', END)
    filename = filedialog.askopenfilename(initialdir="Dataset")
    text.insert(END,filename+" loaded\n\n")
    dataset = pd.read_csv(filename,encoding='iso-8859-1',usecols=['likes','dislikes','view_count','comment_count','description'])
    dataset.fillna(0, inplace = True)
    text.insert(END,str(dataset.head()))


def runwithoutSpark():
    global dataset
    global execution_time
    text.delete('1.0', END)
    execution_time = []
    start = time.time()
    countries = ['pakistan','united states','england','india','sri lanka', 'italy', 'china']
    country_comments = {}
    country_likes = {}
    for i in range(len(countries)):
        country_comments[countries[i]] = 0
        country_likes[countries[i]] = 0
    views_dict = {}
    comment_dict = {}
    likes_dict = {}
    category = ['fashion', 'lifestyle', 'music', 'news', 'comedy', 'entertainment', 'games', 'cartoons']
    for i in range(len(category)):
        views_dict[category[i]] = 0
        comment_dict[category[i]] = 0
        likes_dict[category[i]] = 0
    dataset = dataset.values
    for i in range(len(dataset)):
        like_value = dataset[i,0]
        dislike_value = dataset[i,1]
        comment = str(dataset[i,4])
        comment = comment.strip().lower()
        for j in range(len(countries)):
            if countries[j] in comment:
                if like_value > dislike_value:
                    country_likes[countries[j]] = country_likes.get(countries[j]) + 2
                country_comments[countries[j]] = country_comments.get(countries[j]) + 1
            
    data = []
    columns = ["Type","Countries","Count"]
    for i in range(len(countries)):
        data.append(["Comments",countries[i],country_comments.get(countries[i])])
        data.append(["Likes",countries[i],country_likes.get(countries[i])])
    output = pd.DataFrame(data,columns=columns)
    #text.insert(END,str(output)+"\n\n")
    fig, axs = plt.subplots(1,2)
    output.pivot("Type","Countries", "Count").plot(kind='bar',ax=axs[0])

    for i in range(len(dataset)):
        like_value = dataset[i,0]
        dislike_value = dataset[i,1]
        view_count = dataset[i,2]
        comment = str(dataset[i,4])
        comment = comment.strip().lower()
        for j in range(len(category)):
            if category[j] in comment:
                comment_dict[category[j]] = comment_dict.get(category[j]) + 1
                if view_count > 0:
                    views_dict[category[j]] = views_dict.get(category[j]) + 1
                if like_value > dislike_value:
                    likes_dict[category[j]] = likes_dict.get(category[j]) + 1
    data = []
    columns = ["Type", "Category", "Count"]
    for i in range(len(category)):
        data.append([category[i],"Views",views_dict.get(category[i])])
        data.append([category[i],"Comments",comment_dict.get(category[i])])
        data.append([category[i],"Likes",likes_dict.get(category[i])])
    output = pd.DataFrame(data,columns=columns)
    end = time.time()
    execution_time.append(end-start)
    text.insert(END,"Without spark Execution Time : "+str(end-start)+"\n\n")
    text.update_idletasks()
    output.pivot("Type","Category","Count").plot(kind='bar',ax=axs[1])
    plt.show()


def runwithSpark():
    text.delete('1.0', END)
    global df_frame
    print(df_frame[0])
    global execution_time
    text.delete('1.0', END)
    start = time.time()
    countries = ['pakistan','united states','england','india','sri lanka', 'italy', 'china']
    country_comments = {}
    country_likes = {}
    for i in range(len(countries)):
        country_comments[countries[i]] = 0
        country_likes[countries[i]] = 0
    views_dict = {}
    comment_dict = {}
    likes_dict = {}
    category = ['fashion', 'lifestyle', 'music', 'news', 'comedy', 'entertainment', 'games', 'cartoons']
    for i in range(len(category)):
        views_dict[category[i]] = 0
        comment_dict[category[i]] = 0
        likes_dict[category[i]] = 0
    for i in range(len(dataset)):
        like_value = dataset[i,0]
        dislike_value = dataset[i,1]
        view_count = dataset[i,2]
        comment = str(dataset[i,4])
        comment = comment.strip().lower()
        for j in range(len(category)):
            if category[j] in comment:
                comment_dict[category[j]] = comment_dict.get(category[j]) + 1
                if view_count > 0:
                    views_dict[category[j]] = views_dict.get(category[j]) + 1
                if like_value > dislike_value:
                    likes_dict[category[j]] = likes_dict.get(category[j]) + 1
        for j in range(len(countries)):
            if countries[j] in comment:
                if like_value > dislike_value:
                    country_likes[countries[j]] = country_likes.get(countries[j]) + 2
                country_comments[countries[j]] = country_comments.get(countries[j]) + 1
            
    data = []
    columns = ["Type","Countries","Count"]
    for i in range(len(countries)):
        data.append(["Comments",countries[i],country_comments.get(countries[i])])
        data.append(["Likes",countries[i],country_likes.get(countries[i])])
    output = pd.DataFrame(data,columns=columns)
    #text.insert(END,str(output)+"\n\n")
    fig, axs = plt.subplots(1,2)
    output.pivot("Type","Countries", "Count").plot(kind='bar',ax=axs[0])
    
    data = []
    columns = ["Type", "Category", "Count"]
    for i in range(len(category)):
        data.append([category[i],"Views",views_dict.get(category[i])])
        data.append([category[i],"Comments",comment_dict.get(category[i])])
        data.append([category[i],"Likes",likes_dict.get(category[i])])
    output = pd.DataFrame(data,columns=columns)
    #text.insert(END,str(output))
    end = time.time()
    execution_time.append(end-start)
    text.insert(END,"With spark Execution Time : "+str(end-start)+"\n\n")
    text.update_idletasks()
    output.pivot("Type","Category","Count").plot(kind='bar',ax=axs[1])
    plt.show()


def graph():
    global execution_time
    height = execution_time
    bars = ('Without Spark Execution Time','With Spark Execution Time')
    y_pos = np.arange(len(bars))
    plt.bar(y_pos, height)
    plt.xticks(y_pos, bars)
    plt.title("With & Without Spark Execution Time Comparison")
    plt.show()

def close():
    main.destroy()

font = ('times', 15, 'bold')
title = Label(main, text='Human Behavior Analysis Using Intelligent Big Data Analytics')
title.config(bg='darkviolet', fg='gold')  
title.config(font=font)           
title.config(height=3, width=120)       
title.place(x=0,y=5)

font1 = ('times', 13, 'bold')
ff = ('times', 12, 'bold')

initButton = Button(main, text="Initialize Spark Context", command=initSpark)
initButton.place(x=20,y=100)
initButton.config(font=ff)


uploadButton = Button(main, text="Upload Daily Motion Reviews Dataset", command=uploadDataset)
uploadButton.place(x=20,y=150)
uploadButton.config(font=ff)

withoutSparkButton = Button(main, text="Behaviour Analysis without SPARK", command=runwithoutSpark)
withoutSparkButton.place(x=20,y=200)
withoutSparkButton.config(font=ff)

sparkButton = Button(main, text="Behaviour Analysis with SPARK", command=runwithSpark)
sparkButton.place(x=20,y=250)
sparkButton.config(font=ff)

graphButton = Button(main, text="Execution Time Comparison Graph", command=graph)
graphButton.place(x=20,y=300)
graphButton.config(font=ff)

closeButton = Button(main, text="Exit", command=close)
closeButton.place(x=20,y=350)
closeButton.config(font=ff)


font1 = ('times', 12, 'bold')
text=Text(main,height=30,width=110)
scroll=Scrollbar(text)
text.configure(yscrollcommand=scroll.set)
text.place(x=360,y=100)
text.config(font=font1)

main.config(bg='forestgreen')
main.mainloop()

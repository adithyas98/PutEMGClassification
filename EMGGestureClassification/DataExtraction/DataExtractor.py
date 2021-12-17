#!/usr/bin/env python3

#Created by Adithya Shastry
#Email: ams2590@columbia.edu

#Import stuff

import json #we need to it to save our data
import pandas as pd #to read in hdf5 files
from statistics import mode
import statistics 
import os

class hdfSlidingData:
    '''
    This class will process the putEmg dataset and create a sequential one
    by the use a of a sliding window. After the main processing a json file
    will be created to store data from each run of the window. Windows that 
    contain multuple lables will have one label which is the most common label
    in the window.
    '''
    def __init__(self,window = 200):
        '''
        Inputs:
            - window: the size of the window that will create the temporal
                        region
        '''
        self.window = window
        self.debug = True
        self.errors = 0
    def toJson(self,dataDict):
        '''
        This method will save data and a label to a Json file
        Inputs:
            - dataDict: dictionary to convert to a json
        '''
        return json.dumps(dataDict)
        
    def toDict(self,data,label):
        '''
        Will convert data and labels to a dictionary object that can then be
        used to create a json file
        Inputs:
            - data: lists of lists to save
            - label: the label you want to lists to have
        '''
        dataDict = dict()
        #Add our label to the dict
        dataDict['label'] = label
        #create a sub dict for data points
        dataDict['data'] = dict()
        #now we can iterate through and create our dictionary
        for i,sig in enumerate(data):
            #Cycle through all 24 emg signals
            #we do i+1 to stay consistent with the original dataset
            dataDict['data']["emg{}".format(i+1)] = sig
        #return our dictionary
        return dataDict
    def slidingWindow(self,df,fileOut):
        '''
        Will run the sliding window algorithm on a dataframe passed into it
        Input:
            - df: a dataframe loaded in from the putEMG dataset
            - fileOut: the output file to save the json to
        Output:
            - saves a json file to the hardrive inputted in fileOut
        '''
        #Slice the df
        df = df.iloc[256000:512000,1:25]
        #first extract the columns of the dataframe
        cols = list(df.columns) 
        #get the number of rows in the dataset
        rows = len(df.index)
        #now create our window using self.window
        for i in range(0,rows-self.window):
            #We want to subtract off the window from our range because
                #this will push us outside the bounds of the dataset
            endWindow = i + self.window +1 #the end of the window
            if self.debug:
                print("Reading rows {}-{}".format(i,endWindow))
            #we do plus one since the range method starts at zero and ends
            #   and ends at n-1
            #we want to iterate through the columns and extract the datapoints
            currentData = []
            for c in cols[:-1]:
                #Extract the data points
                currentData.append(df[c][i:endWindow].values.tolist())
            #convert our data to a dictionary
            try:
                #make a copy of the traj_1
                l = df[cols[-1]][i:endWindow].copy().values.tolist()
                dataDict = self.toDict(currentData,mode(l))
            except statistics.StatisticsError:
                print("Data Points were missing for some reason")
                #we want to just skip this window
                self.errors = self.errors + 1
                #check if errors exceeds 10
                if self.errors >= 10:
                    #then we just want to return and continue to the next file
                    return None
                continue

            with open("{}-{}.json".format(fileOut,i),'w') as f:
                f.write(self.toJson(dataDict))
        return None
    def convertHDF5(self,filepath,outputDir):
        '''
        This will run all of the processes and return a list of json objects
        Inputs:
            - filepath: the filepath to a hdf5 file you want to process
            - outputDir: The output filepath
        Output:
            - list of json objects that can be saved
        '''
        #first load the filepath as a dataframe
        df = pd.read_hdf(filepath)
        #Now run the sliding window algorithm on the data
        self.slidingWindow(df,os.path.join(outputDir,filepath.split('.')[0]))
        return None
    def getErrors(self):
        '''
        getter method that returns the number of errors
        '''
        return self.errors
    def resetError(self):
        self.errors = 0

             

        
if __name__ == '__main__':
    window = 1000 #Set a default window size
    slider = hdfSlidingData(window)
    #define file paths
    base = "/Users/adish/Documents/School/Fall 2021 Courses/Deep Learning - Signals/Final Project/EMGGestureClassification/Data/Data-HDF5/sequential"
    os.chdir(base)
    for filename in os.listdir():
        #now iterate through all of the json files and save them
        print(filename)
        out = "jsonOut3"
        outputDir = os.path.join(base,out)
        if not os.path.exists(outputDir):
            #Then make it
            os.mkdir(outputDir)
        slider.convertHDF5(filename,outputDir)
        #print the number of errors
        err = slider.getErrors()
        slider.resetError() #reset the number of errors seen
        if err >= 10:
            print("Skipped to next file {} errors".format(err))

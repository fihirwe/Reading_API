#!/usr/bin/python
import numpy as np
import pandas as pd
import Retreive as retreive
from geopy.geocoders import Nominatim


class Pre_processData(object):

    def __init__(self) :
        super(Pre_processData)
        self.transact, self.custom=retreive.getData()
        self.transactions=[]
        self.customers=[]
        trans_Id=0
        print("Pre-processing the data")
        #putting the formatted transaction in an array
        for trans in self.transact:
            self.transactions.append([trans_Id,trans['customerId'],trans['timestamp'],("%.2f" %trans['amount']),trans['latitude'],trans['longitude']])
            trans_Id+=1
        #retreiving the customer imformation from xml
        for elements in self.custom.findall('customer'):
            id_=elements.find('id').text
            name_=elements.find('name').text
            self.customers.append([id_,name_])
        #putting everything in numpy array
        self.customers= np.array(self.customers)

    def assignTypes(self):
        #transforming to dataframes for easy manipulation
        self.customers_ = pd.DataFrame(self.customers, columns=['Customer_Id', 'Customer_Name'])
        self.transactions_=pd.DataFrame(self.transactions, columns=['Transaction_Id','Customer_Id', 'DateTime','Amount','latitude','longitude'])
        #assigning the right datatypes to columns of the dataframes
        self.customers_.Customer_Id=self.customers_.Customer_Id.astype(int)
        self.customers_.Customer_Name=self.customers_.Customer_Name.astype(str)
        self.transactions_.Customer_Id=self.transactions_.Customer_Id.astype(int)
        self.transactions_.Transaction_Id=self.transactions_.Transaction_Id.astype(int)
        self.transactions_.DateTime=self.transactions_.DateTime.astype(np.int64)
        self.transactions_.Amount=self.transactions_.Amount.astype(float,keep_default_na=False)
        self.transactions_.latitude=self.transactions_.latitude.astype(float)
        self.transactions_.longitude=self.transactions_.longitude.astype(float)
        return self.transactions_, self.customers_

#this method calculation and retrieve the results
def convert(lat_lon):
    geolocator = Nominatim(user_agent="my-application")
    location = geolocator.reverse(lat_lon)
    return location.raw['address']['state']

def processData():
    data=Pre_processData()
    #gettin the processed dataframes 
    transactions_, customers_= data.assignTypes()
    #merging the datasets
    merged_cust_trans=pd.merge(transactions_,customers_, how='left', on=['Customer_Id'])
    #converting the timestamps and formatting
    merged_cust_trans.DateTime=pd.to_datetime(merged_cust_trans.DateTime,unit='ms')
    merged_cust_trans.DateTime=[date1.strftime('%Y-%m-%d %H:%M:%S ') for date1 in merged_cust_trans.DateTime]
    print(merged_cust_trans.DateTime)
    #getting the addresses information in the table
    print("Retreiving the location information.... IF IT FAILS RESTART AGAIN ")

    merged_cust_trans['City_Name']=[convert("'{},{}'".format(a, b)) for a,b in zip(merged_cust_trans.latitude, merged_cust_trans.longitude)]
    tran_fina=merged_cust_trans[['Transaction_Id','DateTime','Customer_Id','Customer_Name','Amount','City_Name']]
    print("Formatting the dataframe to return..")
    city_totals=tran_fina.groupby(['City_Name', 'Customer_Name',])['Amount'].agg(['sum','count']).reset_index()
    city_totals_=city_totals.rename(index=str, columns={"sum": "Total_Amount", "Customer_Name": "Unique_Customers","count":"Total_Transactions"})
    city_totals_=city_totals_[['City_Name','Total_Amount','Unique_Customers','Total_Transactions']]

    print("Writting to csv file..")
    city_totals_.to_csv('city_totals.csv',sep=',',index=False)
    tran_fina.to_csv('Transaction.csv',sep=',',index=False)

    print('Done writting.. checkout the csv files: "Transaction.csv" and "city_totals.csv"')

if __name__ == "__main__":
    processData()

  
        


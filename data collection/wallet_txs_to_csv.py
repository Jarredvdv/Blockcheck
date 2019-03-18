# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 13:35:39 2018

"""
import requests
import json
import time as timer
from datetime import datetime
import csv


def get_txs(address):
    
    # query is of form 'https://blockexplorer.com/api/addrs/$ADDRESS/txs?from=$STARTING_TRANSACTION_NUMBER&to=$ENDING_TRANSACTION_NUMBER
    #max size of 20 transactions
    site_start = 'https://blockexplorer.com/api/addrs/'
    site_mid = '/txs?from='
    site_end = '&to='
    starting_tx = 0
    ending_tx = 20
    
    #make query
    transaction_history = requests.get(''.join([site_start,str(address),site_mid,str(starting_tx),site_end,str(ending_tx)]))
    
    json = transaction_history.json()
    outgoing = []
    incoming = []
    transactions =[]
   
    #query returns total number of transactions for address
    total_txs = int(json['totalItems'])
    
    #while the index of the starting transaction is less than the total number of transactions for the wallet continue making queries
    while(starting_tx < total_txs):
        #attempt to make query
        try:
            transaction_history = requests.get(''.join([site_start,str(address),site_mid,str(starting_tx),site_end,str(ending_tx)]))
            json = transaction_history.json()
        except:
           return('error code 1')
            
        
        for i in json['items']:
            
            try:
                #for each transaction returned extract the transaction id, and the timestamp converted to date form
                tx_id = i['txid']
                time = datetime.utcfromtimestamp(int(i['time'])).strftime('%Y-%m-%d %H:%M:%S')
               
                #loop over inputs and outputs for the transaction and retreive address and associated value for each input/output
                inputs = []
                outputs = []
                for j in i['vin']:
                        inputs.append((j['addr'],j['value']))
                for j in i['vout']:
                        outputs.append((j['scriptPubKey']['addresses'][0],j['value']))
                
                #group transaction data
                transaction = [tx_id,time,inputs,outputs]
    
                #classify each transaction as incoming or outgoing
                out = False
                for input_tup in transaction[2]:
                    if input_tup[0] == address:
                        out = True
                        break
                if out == True:
                    outgoing.append(transaction)
                else:
                    incoming.append(transaction)
            except:
               return('error code 2')
         
        #increase our indices for the next query   
        starting_tx += 20
        ending_tx += 20
        if ending_tx > total_txs:
            ending_tx = total_txs
        
        #print the percent of transactions processed for the wallet
        print(str(round((ending_tx/total_txs)*100))+'%')
    
    #write all outgoing transactions to a csv
    with open(''.join([address,'_outgoing.csv']), mode='w') as outgoing_csv:
        fieldnames = ['tx_id','time','inputs','outputs']
        outgoing_csv = csv.DictWriter(outgoing_csv, fieldnames=fieldnames)
    
        outgoing_csv.writeheader()
        for i in outgoing:
            outgoing_csv.writerow({'tx_id':i[0],'time':i[1],'inputs':i[2],'outputs':i[3]})
   
    #write all incoming transactions to a csv    
    with open(''.join([address,'_incoming.csv']), mode='w') as incoming_csv:
        fieldnames = ['tx_id','time','inputs','outputs']
        incoming_csv = csv.DictWriter(incoming_csv, fieldnames=fieldnames)
    
        incoming_csv.writeheader()
        for i in incoming:
            incoming_csv.writerow({'tx_id':i[0],'time':i[1],'inputs':i[2],'outputs':i[3]})
            
    return('done')

      

#read in a csv
def read_csv(incoming_csv):
    incoming = []
    with open(str(incoming_csv)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader: 
            incoming.append(row)
    return(incoming)



#read in a csv, has extra handeling to remove field labels and skip empty rows
def read_csv_2(incoming_csv):
    incoming = []
    with open(str(incoming_csv)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader: 
            if row != []:
                if row[0] != 'address':
                    incoming.append(row)
    return(incoming)


#read in list of addresses
address_list = read_csv_2('./failed.csv')

#if reading in from an error log the below extracts the address from each line
address_list = [x[0] for x in address_list]

#set starting and ending indices to run from the list
count = 704
end = len(address_list)
failed_address = []

#open error log csv and write header
with open(''.join([datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),'_failures.csv']), mode='w') as fails_csv:
    
    fieldnames = ['address','error code']
    fails_csv = csv.DictWriter(fails_csv, fieldnames=fieldnames)
    fails_csv.writeheader()
    
#print each address and its index      
    for address in address_list[count:end+1]:
        print(address)
        print(count)
        count += 1
        #try to get all transactions, if func returns error code or fails, write address to csv with appropriate error code
        try:
            code = get_txs(address)
            print(code)
                
            if code != 'done':
                failed_address = [address,code]
                fails_csv.writerow({'address':failed_address[0],'error code':failed_address[1]})
        except:
            failed_address = [address,'error code 3']
            fails_csv.writerow({'address':failed_address[0],'error code':failed_address[1]})
           
        print('\n\n\n')

           


   
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 17:43:05 2018

"""
import csv
from datetime import datetime
from collections import Counter
from numpy import median
from numpy import mean
import requests
import json
import ast


def read_csv(incoming_csv):
    incoming = []
    with open(str(incoming_csv)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader: 
            try:
                incoming.append(row[0])
            except:
                pass
    return(incoming)

#csv read that handles field names and empty lines
def read_csv_2(incoming_csv):
    incoming = []
    with open(str(incoming_csv)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader: 
            if row != []:
                if row[0] != 'tx_id':
                    incoming.append(row)
    return(incoming)

#returns difference between last and first transaction in total seconds
def calc_lifetime(incoming,outgoing):
    #retreive the first and last transaction from each list
    #list is in order from latest to oldest
    last_inc = incoming[0][1]
    last_out = outgoing[0][1]
    first_inc = incoming[-1][1]
    first_out = outgoing[-1][1]
    
    #compare dates to determine wallet first and last transaction
    first = first_inc if compare_date(first_inc,first_out) else first_out
    last = last_out if compare_date(last_inc,last_out) else last_inc

    #returns difference between last and first transaction in total seconds
    return((datetime.strptime(last, '%Y-%m-%d %H:%M:%S') - datetime.strptime(first, '%Y-%m-%d %H:%M:%S')).total_seconds())
    
#return true if date_1 is older than date_2    
def compare_date(date_1,date_2):
    date_1 = datetime.strptime(date_1, '%Y-%m-%d %H:%M:%S')
    date_2 = datetime.strptime(date_2, '%Y-%m-%d %H:%M:%S')
    return(date_1<date_2)

#returns a tupple in the form of (most active day, num of txs) and an int for num of active days
def calc_activity_days_and_max_activity(incoming, outgoing):
   #iterate over all transactions appending dates truncated at end of y-m-d to a list
    dates = []
    for x in incoming:
        dates.append(x[1][0:10])
    for x in outgoing:
        dates.append(x[1][0:10])
    
    #create counter for dates and return date with most transactions
    count = Counter(dates)
    for day, c in count.most_common(1):
        max_activity = (day, c)
    
    # return tuple for most active day and length of counter as number of days with a transaction
    return(max_activity, len(count))
 
#return num of incoming transactions / num of outgoing transactions    
def calc_ratio_in_vs_out(incoming,outgoing):
    return(len(incoming)/len(outgoing))   

#return num of incoming transactions
def calc_num_incoming(incoming):
    return(len(incoming))

#return num of outgoing transactions
def calc_num_outgoing(outgoing):
    return(len(outgoing))

#return int of number of unique addresses that have both sent to and received from an address
def calc_num_dif_addresses_recv_send(incoming,outgoing):
    addresses_in = []
    addresses_out = []
    
    #iterate over the inputs of incoming transactions and the outputs of outgoing transactions and append the addresses to a list
    for tx in incoming:
        inc = ast.literal_eval(tx[2])  
        for tup in inc:
            addresses_in.append(tup[0])
    
    for tx in outgoing:
        out = ast.literal_eval(tx[3])
        for tup in out:
            addresses_out.append(tup[0])

    
    #create sets of the list to remove duplicates
    addresses_in = set(addresses_in)
    addresses_out = set(addresses_out)
    
    #count up number of addresses in both lists
    count = 0
    for i in addresses_in:
        if i in addresses_out:
            count+=1
        
    return(count)
    
#returns average, min, max in a float that contains the number of seconds
def calc_delays(incoming,outgoing):
    in_mark = 0
    out_mark = 0
    diff_list = []
    incoming.reverse()
    outgoing.reverse()
    
    #while unparsed transactions exist in either list iterate
    while((in_mark < len(incoming)) and (out_mark < len(outgoing)) ):
        
        #set out date
        out = datetime.strptime(outgoing[out_mark][1], '%Y-%m-%d %H:%M:%S')

        #loop over inputs until the last input before this output is found, if error index is out of range and processing is done so return
        try:
            while(datetime.strptime(incoming[(in_mark+1)][1], '%Y-%m-%d %H:%M:%S')<out):
                in_mark += 1
        except:
            return(median(diff_list).total_seconds(),mean(diff_list).total_seconds(),min(diff_list).total_seconds(),max(diff_list).total_seconds())
        
        #set inc date to this new date
        inc  = datetime.strptime(incoming[in_mark][1], '%Y-%m-%d %H:%M:%S')
        
        #compute the delay
        diff_list.append((out-inc))
        
        #increase in index if out of bounds return
        in_mark += 1
        if in_mark >= len(incoming):
                return(median(diff_list).total_seconds(),mean(diff_list).total_seconds(),min(diff_list).total_seconds(),max(diff_list).total_seconds())
        
        #set new inc date   
        inc  = datetime.strptime(incoming[in_mark][1], '%Y-%m-%d %H:%M:%S')
        
        #loop over outputs until the first output after this input is found, if error index is out of range and processing is done so return
        try:
            while(inc > datetime.strptime(outgoing[(out_mark) ][1], '%Y-%m-%d %H:%M:%S')):
                out_mark += 1
        except:
            return(median(diff_list).total_seconds(),mean(diff_list).total_seconds(),min(diff_list).total_seconds(),max(diff_list).total_seconds())
            

def get_totals(address):
    #make query for totals received/sent by wallet and return as string
    site_start = 'https://blockexplorer.com/api/addr/'
    site_recv = '/totalReceived'
    site_sent = '/totalSent'
    
    transaction_history = requests.get(''.join([site_start,str(address),site_sent]))
    sent = str(transaction_history.json())
    
    transaction_history = requests.get(''.join([site_start,str(address),site_recv]))
    recv = str(transaction_history.json())
    
    sent = "".join([ sent[0:(len(sent)-8)],'.',sent[(len(sent)-8):] ])
    recv = "".join([ recv[0:(len(recv)-8)],'.',recv[(len(recv)-8):] ])
    return(sent,recv)



def trans_avg_and_max_diff(incoming,outgoing,address):
    in_txs = []
    out_txs = []
    
    #convert string of outputs in incoming transactions to python code object
    for tx in incoming:
        inc_outputs = ast.literal_eval(tx[3])       
        
        #for each tuple extract the value and add it to list where the adress is the same as the wallet
        for tup in inc_outputs:
            if tup[0] == address:
                in_txs.append((tx[1],float(tup[1])))
                break
    
    
    
    for tx in outgoing:
        input_val = 0
        output_val = 0
        out_inputs = ast.literal_eval(tx[2])
        
        #convert string of outputs and inputs of outgoing transactions to python code objects
        #iterate over transaction inputs and outputs adding it to the respective total
        for tup in out_inputs:
            if tup[0] == address:
                input_val += float(tup[1])
                
        out_outputs = ast.literal_eval(tx[3])
        for tup in out_outputs:
            if tup[0] == address:
                output_val = float(tup[1])
                break
       
        #value of outgoing transaction is difference between input and output back to the wallet
        out_txs.append((tx[1], float(input_val - output_val)))
    
    #reverse txs lists so that earliest transactions are first
    out_txs.reverse()
    in_txs.reverse()
    
    days = []
    out = []
    inc = []
    out_mark = 0
    in_mark = 0
    

    #iterate over transactions and append all transactions from that day in a 
    #list containg two lists the first of outgoing transactions and the second of incoming transactions
    while((in_mark<len(in_txs)) or (out_mark<len(out_txs))):
        try:
            date = (in_txs[in_mark][0][0:10] if ( datetime.strptime(out_txs[out_mark][0], '%Y-%m-%d %H:%M:%S') < datetime.strptime(in_txs[in_mark][0], '%Y-%m-%d %H:%M:%S')) else out_txs[out_mark][0][0:10])
        except:
            
            try:
                date = in_txs[in_mark][0][0:10]
            
            except:
                date = out_txs[out_mark][0][0:10]
                
        while(in_mark<len(in_txs) and (in_txs[in_mark][0][0:10]==date)):
                inc.append(in_txs[in_mark])
                in_mark += 1
                
        
        while(out_mark<len(out_txs) and (out_txs[out_mark][0][0:10]==date)):
                out.append(out_txs[out_mark])
                out_mark += 1
        
        days.append([out,inc])
    
    
    balance = 0
    
    inc_mark = 0
    out_mark = 0
    max_diff = 0

    day_bals = []    
    
    #for each day compute the maximum and minimum balance of the addres by 
    #processing each transaction in order, if its incoming the new balance is 
    #compared with the max_bal if its outoging the new balance is compared with min_bal

    for day in days:
        max_bal = balance
        min_bal = balance
        
        while((out_mark<len(day[0])) or (inc_mark<len(day[1]))):
            try:
                conditon_1 = (datetime.strptime(day[0][out_mark][0], '%Y-%m-%d %H:%M:%S') < datetime.strptime(day[1][inc_mark][0], '%Y-%m-%d %H:%M:%S'))
            except:
                conditon_1 = True
              
            try:
                conditon_2 = (datetime.strptime(day[0][out_mark][0], '%Y-%m-%d %H:%M:%S') > datetime.strptime(day[1][inc_mark][0], '%Y-%m-%d %H:%M:%S'))
            except:
                conditon_2 = True
                
            while(inc_mark<len(day[1]) and conditon_1 ):
                balance += float(in_txs[inc_mark][1])
                max_bal = max(max_bal, balance)
                inc_mark += 1
                
            while(out_mark<len(day[0]) and conditon_2 ):
                balance -= float(out_txs[out_mark][1])
                min_bal = min(min_bal, balance)
                out_mark += 1
                
         #append date of day and max balance and minimum balance for that day           
        day_bals.append([day[0][0][0][0:10],max_bal,min_bal])
    
    #for each day compute if the next day in the list is the next day timewise
    #if it is compare the max of their max balances minus the min if their 
    #minimum balances to max_diff, if not comare max_diff to days max_bal - min_bal
    for day in range(len(day_bals)):
        try:
            if (((datetime.strptime(day_bals[day+1][0][0:10], '%Y-%m-%d'))  - (datetime.strptime(day_bals[day][0][0:10], '%Y-%m-%d'))).total_seconds() > 86400):
                max_diff = max((day_bals[day][1]-day_bals[day][2]), max_diff)
            else:
                max_diff = max((max(day_bals[day][1],day_bals[day+1][1]) - min(day_bals[day][2] , day_bals[day+1][2])), max_diff)
        except:
            max_diff = max((day_bals[day][1]-day_bals[day][2]), max_diff)
     
    #return mean value of all outgoing transactions, mean value of all incoming transactions, and the maximum difference in balance in a two day window    
    return(mean([x[1] for x in out_txs]), mean([x[1] for x in in_txs]),max_diff)


def generate_features(address):
    #acquire address transaction data
    incoming_csv = "".join(['./failed/',address,'_incoming.csv'])
    outgoing_csv = "".join(['./failed/',address,'_outgoing.csv'])
    incoming = read_csv_2(incoming_csv)
    outgoing = read_csv_2(outgoing_csv)
  
    #generate all features and return
    lifetime = calc_lifetime(incoming,outgoing)
    most_active_day, activity_days = calc_activity_days_and_max_activity(incoming, outgoing)
    in_vs_out = calc_ratio_in_vs_out(incoming,outgoing)
    num_in = calc_num_incoming(incoming)
    num_out = calc_num_outgoing(outgoing)
    addresses_in_out = calc_num_dif_addresses_recv_send(incoming,outgoing)
    median_delay, mean_delay, min_delay, max_delay = calc_delays(incoming,outgoing)
    total_sent, total_recv = get_totals(address)
    avg_out, avg_in, max_diff = trans_avg_and_max_diff(incoming,outgoing,address)
   # print('lifetime', 'most_active_day', 'activity_days','in_vs_out','num_in','num_out','addresses_in_out','median_delay','mean_delay', 'max_delay', 'min_delay','total_sent', 'total_recv','avg_out', 'avg_in', 'max_diff')
    return[lifetime, most_active_day, activity_days,in_vs_out,num_in,num_out,addresses_in_out, median_delay, mean_delay, max_delay, min_delay,total_sent, total_recv,avg_out, avg_in, max_diff]

               
    


#read in address list from csv
address_list = read_csv('./failed/failed.csv')
address_list[0] = address_list[0][3:]

#starting and ending addresses to run
count =0
end = len(address_list)
failed_address = []



#open error log csv and write header
with open(''.join([datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),'_failures.csv']), mode='w') as fails_csv:
    
    fieldnames = ['address','error code']
    fails_csv = csv.DictWriter(fails_csv, fieldnames=fieldnames)
    fails_csv.writeheader()
    
    #open features csv
    with open('non_ponzi_features(3).csv', mode='w') as feat_csv:
    
        fieldnames_2 = ['address','lifetime', 'most_active_day', 'activity_days','in_vs_out','num_in','num_out','addresses_in_out','median_delay', 'mean_delay','max_delay', 'min_delay','total_sent', 'total_recv','avg_out', 'avg_in', 'max_diff']
        feat_csv = csv.DictWriter(feat_csv, fieldnames=fieldnames_2)
        feat_csv.writeheader()     
            
        #iterate over every address and attempt to generate and write features, if an error occurs write address to error log instead
        for address in address_list[count:end+1]:
            print(address)
            print(count)
            count += 1
            
            try:
                features = generate_features(address)
                print(features)
                feat_csv.writerow({'address':address,'lifetime':features[0], 'most_active_day':features[1], 'activity_days':features[2],'in_vs_out':features[3],'num_in':features[4],'num_out':features[5],'addresses_in_out':features[6],'median_delay':features[7],'mean_delay':features[8], 'max_delay':features[9], 'min_delay':features[10],'total_sent':features[11], 'total_recv':features[12],'avg_out':features[13], 'avg_in':features[14], 'max_diff':features[15]})
                #feat_csv.writerow({'address':address,'in_out':features})
            except:
                failed_address = [address,'error']
                fails_csv.writerow({'address':failed_address[0],'error code':failed_address[1]})
            
            print('\n\n\n')       


   











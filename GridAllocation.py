'''
Created on 7 Apr. 2019

@author: nakias

@author: a.robins3

Task:
Your task in this programming assignment is to implement a simple, parallelized application leveraging the
University of Melbourne HPC facility SPARTAN. Your application will search a large geocoded Twitter dataset to
identify Twitter usage around Melbourne and the most frequently occurring hashtags that are being sent from those
areas. 
'''
from mpi4py import MPI
import json
import time
import operator
import os
import sys
start = time.time()
MASTER_RANK =0

#Takes in a file path (melbGrid.json) and returns both a grid_alphabet
#which has the Grid IDs and a Matrix which holds the coordinates of each ID
def create_grid(FILE_PATH):
    f = open(FILE_PATH, 'rb')
    grid_data = json.loads(f.read())
    matrix = []
    ids = []
    grid_alphabet = {}
    grids = grid_data.get('features')
    for each_grid in grids:
        temp_id = each_grid.get('properties').get('id')
        ids.append(temp_id)
    ids.sort()
    count = 0
    for each_id in ids:
        grid_alphabet[count] = each_id
        for each_grid in grids:
            each_id = str(each_id)
            match = str(each_grid.get('properties').get('id'))
            if each_id.endswith(match):
                xmin = each_grid.get('properties').get('xmin')
                xmax = each_grid.get('properties').get('xmax')
                ymin = each_grid.get('properties').get('ymin')
                ymax = each_grid.get('properties').get('ymax')
                matrix.append([xmin, xmax, ymin, ymax])
        count += 1
    return matrix,grid_alphabet

#read_tweets opens the relevent file and starts reading the JSON file line by line
#Then calls the function allocate_grid_posts to analyse each post
def read_tweets(matrix,grid,grid_alphabet,FILE_PATH2):
    json_data = {} 
    with open(FILE_PATH2, 'rb') as somefile:
        for i, line in enumerate(somefile):
            if i != 0:
                try:
                    line = line.strip()
                    json_data['rows'] = []
                    json_data['rows'].append(json.loads(line[:-1]))
                    twitter_data = json_data.get('rows') 
                    value = traverseData(twitter_data)
                    allocate_grid_posts(matrix,grid,grid_alphabet, value[0], value[1])
                except Exception as a:
                    try:
                        json_data['rows'] = []
                        json_data['rows'].append(json.loads(line))
                        twitter_data = json_data.get('rows') 
                        value = traverseData(twitter_data)
                        allocate_grid_posts(matrix,grid,grid_alphabet, value[0], value[1]) 
                    except Exception as b:
                        pass 
    return grid

#traverseData takes each line, extracts information and returns of coordinates and the tweet text
def traverseData (twitterData):
    list_of_coordinates=[]
    list_of_text=[]
    for i in range(0,len(twitterData)):
        json_temp_data=json.dumps(twitterData[i])
        json_temp_data= json.loads(json_temp_data)
        list_of_coordinates.append(json_temp_data.get('doc').get('coordinates').get('coordinates'))
        list_of_text.append(json_temp_data.get('doc').get('text'))
    return [list_of_coordinates,list_of_text]


def allocate_grid_tags(text):
    temp=[]
    text= text.lower()
    text1=text.split()
    for hashtags in text1:

        if hashtags[0]== '#' and len(hashtags)>1:
            temp.append(hashtags)
    return set(temp) 

def add_tags(texts, element, grid_val):
    temp_json = {}
    temp_json['id'] = grid_val
    temp_json['tweets'] = 1
    tag_list = allocate_grid_tags(texts[element])

    temp_json['hashtags'] = []
    tag_json = {}
    if len(tag_list) > 0:
        for tag in tag_list:
            if tag_json.get(tag) == None:
                tag_json[tag] = 1
            else:
                tag_json[tag] = tag_json.get(tag) + 1
    temp_json.get('hashtags').append(tag_json)
    return temp_json

#Takes in a tweet and counts and seperates the hashtags 
#then appends the new results to a temp_json object
def allocate_grid_posts(matrix,grid,grid_alphabet,coordinates,texts):
        for element in range(0,len(coordinates)):
            if coordinates != None:
                for i in range(0, len(matrix)):
                    xmin = matrix[i][0]
                    xmax = matrix[i][1]
                    ymin =matrix[i][2]
                    ymax =matrix[i][3]
                    x = coordinates[element][0]
                    y = coordinates[element][1]
                    if xmin <= x <= xmax and ymin <= y <= ymax:
                        grid_val=grid_alphabet[i]
                        grid_notAdded=True
                        if len(grid.get('data')) > 0:
                            for k in range(0, len(grid.get('data'))):
                                if grid.get('data')[k].get('id') == grid_val:
                                    grid_notAdded=False
                                    grid.get('data')[k]['tweets']=grid.get('data')[k].get('tweets') + 1
                                    tag_list=allocate_grid_tags(texts[element])
                                    for each_tag in tag_list:
                                        if grid.get('data')[k].get('hashtags')[0].get(each_tag) != None:
                                            grid.get('data')[k].get('hashtags')[0][each_tag]=grid.get('data')[k].get('hashtags')[0].get(each_tag)+1
                                        else :
                                            grid.get('data')[k].get('hashtags')[0][each_tag]=1
                                    break;
                            
                        if(grid_notAdded):
                            temp_json={}
                            temp_json = add_tags(texts, element, grid_val)
                            grid.get('data').append(temp_json)
                            break;
                        else:
                            break;                
                          
def get_sorted_ids(output):
    list_of_grids=[]
    for item in output:
        data = item.get('hashtags')[0]
        sorted_d = sorted(data.items(), key=operator.itemgetter(1), reverse=True) 
        temp_list=[item.get('id'),item.get('tweets'),sorted_d[:5]]
        list_of_grids.append(temp_list)
    return list_of_grids      

#Prints the output of the Twitter analysis
def print_output(grid):
    output = grid.get("data")
    output = sorted(output,key = lambda i:i['tweets'],reverse=True)
    list_grids=get_sorted_ids(output)
    for item in list_grids: 
        print ("-"*10)
        print (" Grid id -",item[0], "\tTweets- ",item[1])
        for tag in item[2]:
            print ("\t",tag[0],"-",tag[1])

#merges information from master and slave processors
def master_merge(grids_dict_list,master_grids_dict):
    print ("-"*10,"Merging All Output","-"*10)
    try:
        for slave_grid_dict in grids_dict_list:
            for sgrid in slave_grid_dict['data']:
                for mgrid in master_grids_dict['data']:
                    if mgrid['id'] == sgrid['id']:
                        mgrid['tweets'] += sgrid['tweets']
                        for tag_name,tag_count in sgrid['hashtags'][0].items():
                            if tag_name in mgrid['hashtags'][0].keys():
                                mgrid['hashtags'][0][tag_name] += tag_count
                            else:
                                mgrid['hashtags'][0][tag_name] = tag_count
    except Exception as e:
        print (e)
    return master_grids_dict

#This is the master node which will process one file and then
#merge all results that are sent from the slave processors
def master_tweet_processor(comm, input_file,grids_file):
    rank = comm.Get_rank()
    size = comm.Get_size()
    matrix,grid_alphabet=create_grid(grids_file)

    grid = {'data':[]}
    if len(input_file)> 2 :
        for mfile in input_file:
            master_grid = read_tweets(matrix,grid,grid_alphabet,mfile)
    else:
        master_grid = read_tweets(matrix,grid,grid_alphabet,input_file[0])
    if size > 1:
        grids_dict_list = []
        for i in range(size-1):
            # Send request
            comm.send('return_data', dest=(i+1), tag=(i+1))

        for i in range(size-1):
            # Receive data
            grids_dict_list.append(comm.recv(source=(i+1), tag=MASTER_RANK))

        for i in range(size-1):
            # Receive data
            comm.send('exit', dest=(i+1), tag=(i+1))

        master_grids_dict = master_merge(grids_dict_list,master_grid)
        print_output(master_grids_dict)    
    else:
        print_output(master_grid)
  

#This is the slave, the slave will wait for instruction from master
#to process a file of twitter data        
def slave_tweet_processor(comm,input_file,grids_file):
    rank = comm.Get_rank()
    size = comm.Get_size()
    print ("I am slave")

    matrix,grid_alphabet=create_grid(grids_file)
    grid = {'data':[]}
    # # processes tweets
    slave_grid = read_tweets(matrix,grid,grid_alphabet,input_file)

    # Now that we have our counts then wait to see when we return them.
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        # Check if command
        if isinstance(in_comm, str):
            if in_comm in ("return_data"):
                # Send data back
                comm.send(slave_grid, dest=MASTER_RANK, tag=MASTER_RANK)
            elif in_comm in ("exit"):
                exit(0)

def main():
    file_prefix = sys.argv[1]
    fetch_files = os.popen('ls '+file_prefix+'*').read()
    input_file = fetch_files.split() 
    grids_file = sys.argv[2]
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print ("started")
   
    if size > 1:
      if rank == 0 :
            if len(input_file) > size:
                master_tweet_processor(comm,[input_file[0],input_file[-1]], grids_file)
            else:
                master_tweet_processor(comm,[input_file[0]], grids_file)
      else:
            slave_tweet_processor(comm, input_file[rank],grids_file)
    else:
        master_tweet_processor(comm, [input_file[0]], grids_file)

if __name__ == '__main__':
        main()
        end = time.time()
        print("Time taken",round((end - start),4), "sec")    
            
   


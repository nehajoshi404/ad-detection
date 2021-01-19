

"""# Testing Query"""

# qlambda = rs.QueryLambda.retrieve(
#     'all_audios_stage1',
#     version='0e73cd44f544a0eb',
#     workspace='commons')



# # for interval in range(0, 6000000, 5000):

# params = ParamDict()
# params['interval'] = 5000
# params['object_id'] = "ABC1"
# params['start_time'] = 4000000
# results = qlambda.execute(parameters=params)
# # if len(results['results']) > 0:
# print(results)

# from rockset import Client, ParamDict

# rs = Client()

# # retrieve Query Lambda
# qlambda = rs.QueryLambda.retrieve(
#     'audio_filter',
#     version='0ef91183b98cc555',
#     workspace='commons')

# params = ParamDict()
# params['interval'] = 5000
# params['q_start'] = 4000000
# params['qid'] = "ABC1"
# results = qlambda.execute(parameters=params)
# print(results)

# qlambda = rs.QueryLambda.retrieve(
#     'audio_verify',
#     version='a59b8dfcf21345c8',
#     workspace='commons')

# params = ParamDict()
# params['interval'] = 20000
# params['object_id'] = "ABC1"
# params['offset_bin'] = -3998
# params['offset_win'] = 1
# params['refid'] = 34760659
# params['start_time'] = 4000000
# results = qlambda.execute(parameters=params)
# print(results)

"""# Audio Stage1"""

from rockset import Client, Q, F, P, ParamDict
import time
import datetime
import pandas as pd
import os
rs = Client()

# channel_audio_alias = rs.Alias.retrieve('Audio_Stage1_datetime', workspace='commons')
# channel_audio_alias.drop()

# alias = rs.Alias.create(
#     'Audio_Stage1_datetime',
#     workspace='commons',
#     collections=['commons.20210115_0100-0200'])

# # wait for the alias to be ready
# while not alias.collections:
#     alias = self.rs.Alias.retrieve(
#         alias_name, workspace='commons'
#     )
#     time.sleep(1)

# Audio_Stage1_datetime
# channel_audios_datetime
# 'commons.20210115_0100-0200'
'''
drop_and_create_alias() : to use variable collection_name using alias
1. Drop the alias if it already exists
2. Create a new alias and add the variable collection_name to it
                        eg variable collection_name : 20210115_0100-0200

'''

def drop_and_create_alias( alias_name, collection_name ):


    try : 
        channel_audio_alias = rs.Alias.retrieve(alias_name, workspace='commons')
        channel_audio_alias.drop()
        print("Dropped ", alias_name)
        time.sleep(2)
    except :
        pass


    try : 
        alias = rs.Alias.create(
            alias_name,
            workspace='commons',
            collections=[ str('commons.' + collection_name ) ])

        # wait for the alias to be ready
        while not alias.collections:
            alias = self.rs.Alias.retrieve(
                alias_name, workspace='commons'
            )
            time.sleep(2)
        
        print('Created ', alias_name)

    except : 
        pass

'''
calculate_end_hour()  : to get the end_time in required military format using input start_hour
start_hour         : Should be in 4 digits millitary time
                      eg. 0000,0045,1215,1300,2345
                      1 Hour is divided in 4 halves.
                      Collection_name should be 
                      HHMM where :
                      MM -> 00,15,30,45
'''

def calculate_end_hour(start_hour):

    hours = int(start_hour[0:2])
    mins = int(start_hour[2:])
    mins = mins + 60

    if mins >= 60 : 
        hours = hours + 1
        mins = mins%60      

    if hours < 10 :
        end_time = '0'+ str(hours)
    elif hours == 24:
        end_time = '00'
    else : 
        end_time = str(hours)
  
    if mins < 10 : 
        end_time = end_time + '0' + str(mins)
    else :
        end_time = end_time + str(mins)

    # print(end_time)
    return end_time

# calculate_end_hour('0100')

'''
Audio_Stage_1 
collection_name : collection of 15 mins duration uploaded by merging the audio fingerprinting parquets
                  eg 20210115_0100-0200
query_id        : network id. eg ABC1,ANTEN1 etc
start_time      : start time for queryLambda

1. Retrive the AudioStage1's 15 mins based variable collection if it exists, if not create it.
                        e.g AudioStage1_20210115_0100-0200
2. Run the all_audios_stage1 query and store the results in AudioStage1_collection_name

'''


def add_docs_to_audioStage1( query_id, start_time, max_time, collection_name ):

    qlambda = rs.QueryLambda.retrieve(
        'all_audios_stage1',
        version='0e73cd44f544a0eb',
        workspace='commons')


    overlap = 5000
    params['interval'] = 5000
    params['object_id'] = query_id
    params['start_time'] =  start_time


    # print(' -- QUERY ID ---',query_id)
    # print(qlambda)


    collection_name_audio_stage1 = 'AudioStage1_' + collection_name
    print(collection_name_audio_stage1)
    
    df_list = []
    try:
        rs.Collection.retrieve(collection_name_audio_stage1)
    except:
        rs.Collection.create(collection_name_audio_stage1)
        time.sleep(2)


    while  params['start_time'] < max_time :

        result = qlambda.execute(parameters = params).results

        if params['start_time'] % 500000 == 0: 
            print((query_id) + ' - start time - '+ str(params['start_time']))

        params['start_time'] = params['start_time'] + overlap
        print(result, params)

        if len(result) > 0:

            print('Result {}'.format(result))

            # for row in result:
            #   df_list.append( row )

            ret = rs.Collection.add_docs( collection_name_audio_stage1 , result )

    # df = pd.DataFrame(df_list)
    # print(df)

# params['interval'] = 5000
# params['object_id'] = "ABC1"
# params['start_time'] = 4000000
# collections=['commons.20210115_0100-0200'])

# add_docs_to_audioStage1( query_id="ABC1", start_time=4000000, max_time=4100000, date="20210115", start_hour_of_date="0100" )

"""# Verify Audio Stage 1 Matches"""

'''
Verify_Audio_Stage_1 

1. Retrive the VerifiedAudio's 15 mins based variable collection if it exists, if not create it.
                        e.g VerifiedAudio_20210115_0100-0200
2. Run the audio_filter and audio_verify queries and store the results in VerifiedAudio_collection_name

'''


def filter_and_verify_audio_matches( query_id, start_time, max_time, collection_name ) :


    qlambda_audio_filter = rs.QueryLambda.retrieve(
        'audio_filter',
        version='0ef91183b98cc555',
        workspace='commons')

    qlambda_audio_verify = rs.QueryLambda.retrieve(
        'audio_verify',
        version='a59b8dfcf21345c8',
        workspace='commons')

    params = ParamDict()
    params2 = ParamDict()


    params['qid'] = query_id
    params['interval'] =  5000
    params['q_start'] = start_time


    collection_name_verified = 'VerifiedAudio_' + collection_name
    print(collection_name_verified)


    try:
        print("\tRetrieve ")
        rs.Collection.retrieve( collection_name_verified )
    except:
        print("\tCreate ")
        rs.Collection.create( collection_name_verified )
        time.sleep(2)


    df_list = []
    while  params['q_start'] < max_time :
        
        filter_results = qlambda_audio_filter.execute(parameters=params).results

        if params['q_start'] % 500000 == 0: 
            print((query_id) + ' - q_start - '+ str(params['q_start']))

        params['q_start'] = params['q_start'] + params['interval']
        # print (filter_results)

        for res in filter_results:

            print ('Filtered Result -> ',res)

            params2['object_id'] = res['query']
            params2['interval'] = res['ref_duration']
            params2['refid'] = res['rid']
            params2['start_time'] = res['minq']
            params2['offset_bin'] = res['offset_bin']
            params2['offset_win'] = 1
            if res['ref_duration'] > 35000:
                params2['offset_win'] = 2 
            params2['hits'] = res['hits']
            params2['mcnt'] = res['mcnt']

            verify_result = result = qlambda_audio_verify.execute(parameters=params2).results

            print ("Verified result -> ")
            print('\t',verify_result)
            # df_list.append(verify_result[0])

            if len(verify_result):
                ret = rs.Collection.add_docs( collection_name_verified , verify_result )

    return collection_name_verified        

    # df = pd.DataFrame(df_list)
    # print(df)

def get_rows_from_rockset_Verified_table( channel_id, query_start, query_end,collection_name_verified ):

    query_f = Q('select chan_start, q_start, q_end, cnt,  rid, ref_duration, valid, qid from {collection_name_verified} ').where( F['cnt']>= 0.5 and F['qid'] == {channel_id} )
    results = rs.sql(query_f).results()

    return results



def ad_detect_main( query_id, start_time, max_time, date, start_hour_of_date ) :


    end_hour_of_date = calculate_end_hour( str(start_hour_of_date) )
    collection_name = str(date) + '_' + str(start_hour_of_date) + '-' + end_hour_of_date
    print(collection_name)

    drop_and_create_alias('channel_audios_datetime', str(collection_name) )
    drop_and_create_alias( 'Audio_Stage1_datetime' , str(collection_name) )

    add_docs_to_audioStage1( query_id, start_time, max_time, str(collection_name) )
    time.sleep(1)
    collection_name_verified = filter_and_verify_audio_matches( query_id, start_time, max_time, collection_name )

    '''
    saving directory structures 20210115_0100-0200/ABC1.csv 

    '''



    list_channels = []
    for channel in list_channels:

    	csv_path = os.path.join(collection_name,str(channel) + ".csv")

        results = get_rows_from_rockset_Verified_table(channel,start_time,end_time,collection_name_verified)
        df = pd.DataFrame(results)

        df.to_csv(csv_path, columns=['chan_start', 'q_start', 'q_end', 'cnt',  'rid', 'ref_duration', 'valid', 'qid'])






rs = Client()

ad_detect_main( query_id="ABC1", start_time=4000000, max_time=4100000, date="20210115", start_hour_of_date="0100" )
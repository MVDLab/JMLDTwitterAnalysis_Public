import os
import pandas as pd
import numpy as np
#import csv

#import tweet date from CSV file
os.chdir('ENTER FILE LOCATION PATH HERE')
filename = 'ENTER FILE NAME HERE.csv'
tweets = pd.read_csv(filename, header=0)

##Determine number of overall tweets in the dataset
tweets.shape[0]
tweets['like_count'].sum()
tweets['retweet_count'].sum()
tweets['reply_count'].max()
tweets['quote_count'].sum()


##Determine number of unique accounts contained in the dataset
tweets_unique = tweets.drop_duplicates(subset=['author id']) #only drops duplicate author IDs
tweets_unique = tweets_unique.reset_index()
tweets_unique = tweets_unique.drop('index', axis=1)
tweets_unique.shape[0]


#Set terms to use to search by
DCDterms = ['dyspraxia','dyspraxic','developmental coordination disorder','DCD','clumsy']
Autterms = ['autistic','actuallyautistic','autism','autist','autie','asperger','aspie','ASD']

##Determine number of autistic OR DCD/dyspraxia OR (autistic AND DCD/dyspraxia) accounts are in the dataset
#Create new column in tweets_unique that will contain the specific group
tweets_unique['group'] = np.nan
groupcol = tweets_unique.columns.get_loc("group")

#Label account by terms in bio
for tweetnum in range(0, len(tweets_unique)):
    try:
        #print(tweetnum, tweets_unique['bio'][tweetnum])
        if any(DCD.casefold() in str(tweets_unique['bio'][tweetnum]).casefold() for DCD in DCDterms) and any(Aut.casefold() in str(tweets_unique['bio'][tweetnum]).casefold() for Aut in Autterms):  # Find if has Autistic and Dyspraxia
            tweets_unique.iloc[tweetnum, groupcol] = 'AutDCD'  # assign AutDCD to account
        elif any(DCD.casefold() in str(tweets_unique['bio'][tweetnum]).casefold() for DCD in DCDterms): # Find if has Dyspraxia only
            tweets_unique.iloc[tweetnum, groupcol] = 'DCD'  # assign DCD to account
        elif any(Aut.casefold() in str(tweets_unique['bio'][tweetnum]).casefold() for Aut in Autterms):  # Find if has Autistic only
            tweets_unique.iloc[tweetnum, groupcol] = 'Aut'  # assign Aut to account
        else:  # Find if has Neither
            tweets_unique.iloc[tweetnum, groupcol] = np.nan  # assign NAN to tweets_unique.loc['group'][current row?]
    except KeyError:
        print(tweetnum, tweets_unique['bio'][tweetnum])
#Count account numbers by Group label
GroupCount = tweets_unique.groupby('group').count()['author id']
print(GroupCount)

##Count keyword overlap 
#Create new column in tweets_unique that will contain the specific group
tweets['label'] = np.nan
labelcol = tweets.columns.get_loc("label")

#Label tweets by terms in tweet text
for tweetnum in range(0, len(tweets)):
    try:
        #print(tweetnum, tweets_unique['bio'][tweetnum])
        if any(DCD.casefold() in str(tweets['tweet'][tweetnum]).casefold() for DCD in DCDterms) and any(Aut.casefold() in str(tweets['bio'][tweetnum]).casefold() for Aut in Autterms):  # Find if has Autistic and Dyspraxia
            tweets.iloc[tweetnum, labelcol] = 'AutDCD'  # assign AutDCD to tweet
        elif any(DCD.casefold() in str(tweets['tweet'][tweetnum]).casefold() for DCD in DCDterms): # Find if has Dyspraxia only
            tweets.iloc[tweetnum, labelcol] = 'DCD'  # assign DCD to tweet
        elif any(Aut.casefold() in str(tweets['tweet'][tweetnum]).casefold() for Aut in Autterms):  # Find if has Autistic only
            tweets.iloc[tweetnum, labelcol] = 'Aut'  # assign Aut to tweet
        else:  # Find if has Neither
            tweets.iloc[tweetnum, groupcol] = np.nan  # assign NAN to tweets_unique.loc['group'][current row?]
    except KeyError:
        print(tweetnum, tweets['tweet'][tweetnum])

LabelCount = tweets.groupby('label').count()['author id']
print(LabelCount)

tweets.to_csv('Tweets_Labelled.csv',sep=',',index=False)

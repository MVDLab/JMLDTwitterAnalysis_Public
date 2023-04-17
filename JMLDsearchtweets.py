# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import dateutil.parser
# To add wait time between requests
import time

# auth() function - will retrieve the token from the environment
def auth():
    return os.getenv('TOKEN')

# function that will take our bearer token, pass it for authorization, and return headers to use and access the API
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# build the request by defining the endpoint to connect to and use parameters to pass to the endpoint
def create_url(keyword, start_date, end_date, max_results = 500):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" 

    #remember to change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)
    

# function below wil send a "GET" request and will return the response in "JSON" format
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    #print(params)
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()
    
# append_to_csv function will append all the data we collect to a CSV file of our desired name
def append_to_csv(json_response, fileName):
    #A counter variable
    counter = 0
    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)
    # Loop through each tweet
    for tweet in json_response['data']:
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that
        # 1. Author ID
        author_id = tweet['author_id']
        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])
        # 3. Geolocation
        if ('geo' in tweet):
            geo = tweet['geo']['place_id']
        else:
            geo = " "
        # 4. Tweet ID
        tweet_id = tweet['id']
        # 5. Language
        lang = tweet['lang']
        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']
        # 7. source
        source = tweet['source']
        # 8. Tweet text
        text = tweet['text']
        description = 'Error'
        for user in json_response['includes']['users']:
            tempid = user['id']
            if tempid == author_id:
                description = user['description']
        # Assemble all data in a list
        res = [author_id, description, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source,
               text]
                #description,
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1
    # Print the number of tweets for this iteration
    print("# of Tweets added from this request: ", counter)
    
    

#  Assign user/app bearer token
os.environ['TOKEN'] = 'ENTER YOUR BEARER TOKEN HERE'

# Inputs for the request
bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "ENTER YOUR KEYWORDS HERE"
start_time = "ENTER YOUR START TIME HERE"
end_time = "ENTER YOUR END TIME HERE"
max_results = 500

#  Set file location
os.chdir('ENTER YOUR FILE LOCATION PATH HERE')

# create filename for saving data
filename = 'ENTER YOUR FILE NAME HERE.csv'

# Create file
csvFile = open(filename, "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)

#Create headers for the data you want to save, in this example, we only want save these columns in our dataset
csvWriter.writerow(['author id', 'bio', 'created_at', 'geo', 'tweet id', 'lang', 'like_count', 'quote_count', 'reply_count', 'retweet_count', 'source', 'tweet'])
csvFile.close()

#create URL for endpoint request
url = create_url(keyword, start_time, end_time, max_results)

# Add loop that runs until next_token does not exist and causes error, sleep if running for 300 seconds
requestnum = 1
print('Starting Request #', requestnum)
next_token = None
json_response = connect_to_endpoint(url[0], headers, url[1], next_token) #first request
next_token = json_response['meta']['next_token']  # pull next token from here
append_to_csv(json_response, filename)
print('Ending Request #', requestnum)
while next_token:
    # sleep function pauses the script; full archive search - 300 requests/15 min
    if requestnum == 300:
        print("Start : %s" % time.ctime())
        time.sleep(601)
        requestnum = 0
        print("End : %s" % time.ctime())
    print('Starting Request #', requestnum)
    print(next_token)
    # runs through search tweets python, calls connect_to_endpoint and prints out json_response
    json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
    requestnum += 1
    time.sleep(1)  # pause for 1 second to not overwhelm endpoint
            

    #Append tweets to csvfile
    append_to_csv(json_response, filename)
    try:
        next_token = json_response['meta']['next_token']  # pull next token from here
        print('Ending Request #', requestnum)
    except KeyError:  # if next_token does not exist and throws keyerror, end, close csv, and break loop
        print('Ending Request #', requestnum)
        csvFile.close()
        break

# When done, close the CSV file
csvFile.close()




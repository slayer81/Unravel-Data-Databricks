######################################################################################################
# WARNING! This script is provided WITHOUT WARRANTY
# While we have taken reasonable measures to ensure execution safety, we strongly advise that you
# DO NOT RUN IT ON PRODUCTION SYSTEMS until you have tested it against your corporate polices.
#
# Author:           Scott Lutz
# Version:          1.0
# Release Date:     25 Sept, 2024
######################################################################################################
import requests
import os
import json
from requests.exceptions import HTTPError
from datetime import timedelta, datetime

# This block contains parameters you may want to customise
##################################################################
# Change to True if you want all duplicate Insights, for impact assessment
keep_Insight_duplicates = False

# For debugging or increased output verbosity, set to True
debugMode = False

# dataDir => Define the directory location to save the report to
# dataDir = './Data'
# DATADIR => Define the directory location to save the report to
SCRIPTNAME = os.path.splitext(os.path.basename(__file__))[0]
dataDir = f'Data/{SCRIPTNAME}'

# Check if this dir exists. Create if not
os.makedirs(dataDir, exist_ok=True)

# Credentials (__Not yet implemented__)
# Choice of:
#   'env': pull from user environment variables
#   'file': read username and password from credentials file
credType = 'env'

USERNAME = os.getenv('unravel_open_user', None)
PASSWORD = os.getenv('unravel_open_pass', None)

# Unravel polling frequency period => frequency script will run, to avoid data duplication
# Choose from: ['seconds', 'minutes', 'hours', 'days', 'weeks']
unravelPollFreqUnit = 'days'
unravelPollFreqValue = 90

# Target platform
# Choose from: ['emr', 'dataproc', 'bigquery', 'databricks', 'snowflake']
platform = 'databricks'

# Jobs scope
# Choose from: ['all', 'finished', 'running', 'inefficient']
myScope = 'all'

# Workspaces scope
myWorkspaces = []

# URLs for each specific platform
urlsDict = {
    'databricks': 'https://playground-databricks.unraveldata.com'      # <----- Enter your url here
}

# The activity types to search for. You can remove any not required
# appTypes = ['spark', 'impala', 'hive', 'mr', 'tez', 'bigquery']
appTypes = ['spark']
##################################################################
# End of customised parameter section


start_time = datetime.now()

##################################################################
# Error codes
errorCodesDict = {
    0: 'API: Your query returned 0 results',
    1: 'Date/Time: Value Out of Bounds',
    3: 'API: Your query resulted in an unknown error state',
    400: 'Authentication failed: Invalid credentials',
    405: 'API: Your target endpoint has an invalid configuration'
}

# Status map
statusMapDict = {
    'K': 'Killed',
    'F': 'Failed',
    'R': 'Running',
    'S': 'Success',
    'P': 'Pending',
    'U': 'Unknown',
    'W': 'Waiting'
}
appStatus = list(statusMapDict.keys())
#################################################################################################


# Begin User Defined Functions
#################################################################################################
def validate_poll_frequency(unit: str, value: int, stage_num: int):
    if debugMode:
        print("\nStage {}:\tValidating parameters: \"Poll Frequency\"".format(stage_num))
    unravelPollFreqRangeDict = {
        'seconds': list(range(1, 300)),     # Acceptable range 1 - 299
        'minutes': list(range(1, 720)),     # Acceptable range 1 - 719
        'hours': list(range(1, 48)),        # Acceptable range 1 - 47
        'days': list(range(1, 91)),         # Acceptable range 1 - 90
        'weeks': list(range(1, 5))          # Acceptable range 1 - 4
    }

    if value not in unravelPollFreqRangeDict[unit]:
        message = 'Stage {}:\t Value set for "Poll Frequency" ({}) is outside the acceptable range: ({} - {})'.format(
            stage_num, unravelPollFreqValue, min(unravelPollFreqRangeDict[unit]), max(unravelPollFreqRangeDict[unit])
        )
        print(message)
        exit(errorCodesDict[9])
    else:
        if debugMode:
            print("Stage {}:\tAcceptable parameters for \"Poll Frequency\"".format(stage_num))

    lookback_dict = {
        unit: value
    }
    return lookback_dict
#################################################################################################


#################################################################################################
def get_zulu_datetime_now():
    return str(datetime.utcnow().isoformat())[:-3] + 'Z'
#################################################################################################


#################################################################################################
def format_milliseconds(value):
    # Convert milliseconds to seconds
    seconds = value // 1000
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    # Build the output string
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0:
        parts.append(f"{seconds}s")
    result = " ".join(parts)
    return result
##################################################################


##################################################################
def subtract_timedelta_from_now(poll_freq_dict, stage_num: int):
    # Subtract 'poll_freq_dict' from the current datetime
    new_dt = datetime.utcnow() - timedelta(**poll_freq_dict)

    # Convert the new date to an ISO zulu-formatted string
    new_date_str = str(new_dt.isoformat())[:-3] + 'Z'
    if debugMode:
        print("Stage {}:\tStart time dt: {}".format(stage_num, new_date_str))
        print("Stage {}:\tEnd Time dt:   {}".format(stage_num, str(datetime.utcnow().isoformat())[:-3] + 'Z'))
    return new_date_str
#################################################################################################


#################################################################################################
def get_auth_token(stage_num: int):
    if debugMode:
        print("Stage {}:\tAttempting to generate auth token".format(stage_num))
    global platform
    # Create a dictionary with the username and password stored in $USER_ENV
    authDict = {
        'username': USERNAME,
        'password': PASSWORD
    }
    # endpoint_url = '{}/api/v1/webSignInw'.format(urlsDict[platform])      # Used for testing HTTP status codes
    endpoint_url = '{}/api/v1/webSignIn'.format(urlsDict[platform])

    # POST user credentials to the sign-in endpoint
    response = requests.post(
        endpoint_url,
        data=authDict,
        verify=True
    )

    # Check the response code and presence of auth token
    if response.status_code == 200 and response.json()['token']:
        auth_token = 'JWT {}'.format(response.json()['token'])
        if debugMode:
            print("Stage {}:\tSuccessfully generated authentication token!")
        return auth_token
    elif response.status_code == 400:                                       # Bad Credentials
        exit(errorCodesDict[400])
    elif response.status_code == 405:                                       # Invalid API endpoint configuration
        exit(errorCodesDict[405])
    else:                                                                   # Unknown error catch-all
        exit(errorCodesDict[3])
#################################################################################################


#################################################################################################
def write_list_of_dicts_to_csv(data, stage_num: int):
    import csv
    field_names = list(data[0].keys())
    csv_filename = f'{SCRIPTNAME}-{datetime.utcnow().strftime("%Y-%m-%d_%H.%M.%S")}.csv'
    datafile_full_path = os.path.join(dataDir, csv_filename)
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', f'Writing {len(data)} rows of data to CSV', ''))

    # Write data to file
    try:
        with open(datafile_full_path, mode='w') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(data)
            output_file.close()
        print(f'Stage {stage_num}:\t Successfully output data to:\t "{datafile_full_path}"')
        print('Stage {:<6} {:<70} {:>10}'.format(
            f'{stage_num}:', f'Output data to: "{datafile_full_path}"', 'SUCCESS')
        )
    except Exception as e:
        print(f'Stage {stage_num}:\t Failed to write data to CSV file:\t "{datafile_full_path}"')
        print(f'Stage {stage_num}:\t Message:\t "{str(e)}"')
        if debugMode:
            print(f'Stage {stage_num}:\t Field names:\t "{field_names}"')
            print(f'Stage {stage_num}:\n Data:\t {data}')
#################################################################################################


#################################################################################################
def get_cluster_detail(headers_dict, count: int, poll_freq_dict, stage_num: int):
    if debugMode:
        print("Stage {}:\tFetching Cluster detail".format(stage_num))
    # Construct the UnifiedSearch API URL
    endpoint_url = '{}/api/v1/apps/unifiedsearch'.format(urlsDict[platform])

    # Construct request parameters
    paramsDict = {
        'from': 0,
        'size': count,
        'start_time': subtract_timedelta_from_now(poll_freq_dict, stage_num),
        'end_time': str(datetime.utcnow().isoformat())[:-3] + 'Z',  		# endTime in ISO zulu format
        'appStatus': appStatus,
        'appTypes': appTypes,
        'queryOnFinishedTime': False
    }

    # Query UnifiedSearch API to get cluster data
    response = requests.post(
        endpoint_url,
        data=json.dumps(paramsDict),
        verify=True,
        headers=headers_dict)

    # Check the response status code
    if response.status_code == 200 and response.json()['metadata']['totalRecords'] > 0:
        resultsList = response.json()['results']
        if debugMode:
            print("Stage {}:\tSuccessfully fetched data of {} clusters".format(stage_num, len(resultsList)))
        return resultsList
    elif response.status_code == 200 and response.json()['metadata']['totalRecords'] == 0:
        exit(errorCodesDict[1])
    elif response.status_code == 422:
        responseDict = response.json()
        print("Stage {}:\tQuery Response: {}\n\tExiting!!".format(stage_num, responseDict['error'][0]['message']))
        exit(errorCodesDict[1])
    elif response.status_code == 405:
        exit(errorCodesDict[4])
    else:
        responseDict = response.json()
        print("Stage {}:\tQuery Response: ({}) {}\n\tExiting!!".format(stage_num, response.status_code, responseDict))
        exit(errorCodesDict[3])
#################################################################################################


#################################################################################################
def parse_cluster_data(data, headers_dict):
    fieldnames_dict = {
        "status_long": "Status",
        "name": "Cluster Name",
        "runName": "Job Name",
        "clusterType": "Cluster Type",
        "raw_user": "User",
        "queue": "Workspace",
        "start_time": "Start Time",
        "setupDuration": "Setup Duration",
        "duration_long": "Duration",
        "cost": "Cost",
        "dbus": "DBUs"
    }

    cluster_info_list = []
    for cluster in data:
        # Filter out all the items we don't need
        key = {cluster_k: cluster_v for cluster_k, cluster_v in cluster.items() if cluster_k in fieldnames_dict.keys()}
        temp_dict = {}
        # for field in target_values_list:
        for k in fieldnames_dict.keys():
            dict_key = fieldnames_dict[k]
            value = key.get(k)
            # Check if the field is empty and set "N/A" for strings
            if isinstance(value, str) and not value:
                temp_dict[dict_key] = "-"
            # Set None for missing or empty float fields
            elif isinstance(value, (float, int)) and (value is None or value == 0.0):
                temp_dict[dict_key] = None
            else:
                temp_dict[dict_key] = value

        # Format the "Start Time" value as required
        if not temp_dict['Start Time'] == "-":
            key_date, key_time = temp_dict['Start Time'].split(sep=None, maxsplit=1)
            temp_dict['Start Time'] = key_time + ', ' + key_date

        # format the "Setup Duration" value as required
        if not temp_dict['Setup Duration'] is None:
            try:
                temp_dict['Setup Duration'] = format_milliseconds(temp_dict['Setup Duration'])
            except Exception as e:
                print(f"Setup Duration value:\t {temp_dict['Setup Duration']}")

        # format the "Duration" value as required
        if not temp_dict['Duration'] is None:
            try:
                temp_dict['Duration'] = format_milliseconds(temp_dict['Duration'])
            except Exception as e:
                print(f"Duration value:\t {temp_dict['Duration']}")

        # Format the "Cost" value as required
        if not temp_dict['Cost'] is None:
            try:
                temp_dict['Cost'] = round(temp_dict['Cost'], 2)
            except Exception as e:
                print(f"Cost value:\t {temp_dict['Cost']}")

        # Format the "DBUs" value as required
        if not temp_dict['DBUs'] is None:
            try:
                temp_dict['DBUs'] = round(temp_dict['DBUs'], 2)
            except Exception as e:
                pass

        # Fetch cluster "Insights"
        cluster_dict = {
            'platform_url': urlsDict[platform],
            'clusterUid': cluster['clusterUid'],
            'id': cluster['id']
        }
        insight_values = get_cluster_insights_by_app(headers_dict, cluster_dict)

        # Remove duplicate Insights
        if not keep_Insight_duplicates:
            insight_values = list(dict.fromkeys(insight_values))

        # Add "Insights" to dict
        for val in insight_values:
            new_dict = temp_dict.copy()
            new_dict['Insights'] = val
            cluster_info_list.append(new_dict)
    return cluster_info_list
#################################################################################################


#################################################################################################
def get_cluster_insights_by_app(headers_dict, cluster_dict):
    # target_keys = ['Efficiency', 'Bottlenecks', 'appFailure']
    # Construct Spark Job Analysis API URL
    url = f'{cluster_dict["platform_url"]}/api/v1/spark/{cluster_dict["clusterUid"]}/{cluster_dict["id"]}/1/analysis'

    # GET request from Spark Job Analysis API
    response = requests.get(url, headers=headers_dict)

    return_values = []
    if response.status_code == 200:
        json_response = json.loads(response.text)['insightsV2']
        if debugMode:
            print(f'  *** Insights for:\t {response.url}')
            print(f'\t {json_response}')
        if not isinstance(json_response, list):
            return return_values
        else:
            for i in json_response:
                # Loop through the keys under 'categories' and access nested values within 'instances'
                for category_key, category_value in i['categories'].items():
                    for instance in category_value['instances']:
                        if not category_key:
                            # If category_key is empty, this is not an Insight
                            continue
                        results = instance['title']
                        return_values.append(results)
    return return_values
#################################################################################################
# End User Defined Functions


#################################################################################################
def main():
    stage_num: int = 1

    ################################################
    # Stage 1: Validate Poll Frequency parameters
    ################################################
    print('Stage {:<6} {:<70}'.format(f'{stage_num}:', 'Validating Poll Frequency parameters'))
    pollFreqDict = validate_poll_frequency(unravelPollFreqUnit, unravelPollFreqValue, stage_num)
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', 'Validating Poll Frequency parameters', 'SUCCESS'))
    stage_num += 1

    ################################################
    # Stage 2: Generate Unravel auth_token from UI credentials
    ################################################
    print('Stage {:<6} {:<70}'.format(f'{stage_num}:', 'Generating authentication token'))
    authToken = get_auth_token(stage_num)
    headersDict = {
        'Authorization': authToken,
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', 'Generating authentication token', 'SUCCESS'))
    stage_num += 1

    ################################################
    # Stage 3: Get Cluster detail
    ################################################
    print('Stage {:<6} {:<70}'.format(f'{stage_num}:', 'Collecting cluster details'))
    clusterCount = 10000
    clusterDataList = get_cluster_detail(headersDict, clusterCount, pollFreqDict, stage_num)
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', 'Collecting cluster details', 'SUCCESS'))
    stage_num += 1

    ################################################
    # Stage 4: Get Cluster Insights
    ################################################
    print('Stage {:<6} {:<70}'.format(f'{stage_num}:', 'Collecting cluster Insights'))
    clusterData = parse_cluster_data(clusterDataList, headersDict)
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', 'Collecting cluster Insights', 'SUCCESS'))
    stage_num += 1

    ################################################
    # Stage 5: Write data to CSV file
    ################################################
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', f'Writing final report data to CSV', ''))
    write_list_of_dicts_to_csv(clusterData, stage_num)
    print('Stage {:<6} {:<70} {:>10}'.format(f'{stage_num}:', 'Writing final report data to CSV', 'SUCCESS'))
    stage_num += 1
#################################################################################################


if __name__ == "__main__":
    main()

print("Total execution time: {}".format(datetime.now() - start_time))

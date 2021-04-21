import os
import json
import uuid
import collections
import pandas as pd

#function to check if UUID is valid
def is_valid_uuid(val):
    """
    This function is responsible for

    * validating uuid for linkid in the json
    * This function primarily calls UUID function from uuid library
    
    :param str val: linkid
    :param str exception: Exception string if any
    :return: Boolean value, True if linkid is valid and False for invalid
    """

    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


def valid_dataframe(df):
    """
    This function is responsible for

    * creating separate dataframe for valid and invalid uuid
    * This function primarily applies condition on each row of dataframes
    
    :param dataframe object
    :return: valid and invalid uuid dataframes
    """
    #checking if UUID is valid and creating separate dataframe for valid and invalid uuid
    df['uuid_status'] = ["valid" if is_valid_uuid(ele) == True else "invalid" for ele in df["linkid"]]

    valid_uuid_df = df[df['uuid_status'].str.match('valid')]
    invalid_uuid_df = df[df['uuid_status'].str.match('invalid')]

    return valid_uuid_df, invalid_uuid_df


def dump_notnull_df(valid_uuid_df, email_meta):
    """
    This function is responsible for

    * converting the rates to USD based on currency and creating separate json file for it
    * This function creates a json file of rate conversion data
    
    :param dataframe object : valid_uuid_df
    :param json object : email_meta
    :return: json file with name rate_conversion.json
    """
    cols = ['convvalue', 'convvalueunit']

    #checking if 'convvalue', 'convvalueunit' are not null
    notnull_df = valid_uuid_df[valid_uuid_df[cols].notnull().all(1)]

    #creating a new column which stores the currency value based on rates
    notnull_df['currency_value'] = [email_meta.get(ele) for ele in notnull_df["convvalueunit"]]

    #converting the rates to USD based on currency and creating separate json file for it
    if not notnull_df.empty:
        notnull_df['convusdvalue'] = notnull_df['convvalue'] * notnull_df['currency_value']
        notnl_data = notnull_df.to_dict('records')
        with open('rate_conversion.json', 'w') as fout:
            json.dump(notnl_data, fout)


def similar_events(valid_uuid_df):
    """
    This function is responsible for

    * clustering data of similar events
    * This function is responsible to create json files with same events
    
    :param dataframe object
    :return: json files with event names
    """
    #splitting valid uuid data based on similar types
    rec = valid_uuid_df.to_dict('records')
    result = collections.defaultdict(list)
    
    for d in rec:
        result[d['type']].append(d)

    result_list = list (result.values())

    #creating json file of similar type data
    for ele in result_list:
        with open(ele[0]['type'] + '.json', 'w') as fout:
            json.dump(ele[0], fout)


if __name__ == "__main__":

    #reading both rates.json and data_engineer.json
    cur_dir = os.path.dirname(os.path.abspath(__file__))

    rate_file = os.path.join(cur_dir, 'rates.json')
    with open(rate_file) as rf:
        email_meta = json.load(rf)
    
    data_file = os.path.join(cur_dir, 'data.json')
    with open(data_file) as ef:
        json_data = [json.loads(line) for line in ef]

    df = pd.DataFrame(json_data)
    
    valid_uuid_df, invalid_uuid_df = valid_dataframe(df)

    #dumping invalid UUID data to a json file
    if not invalid_uuid_df.empty:
        inv = invalid_uuid_df.to_dict('records')
        with open('deadletters.json', 'w') as fout:
            json.dump(inv, fout)

    #currency converted data
    dump_notnull_df(valid_uuid_df, email_meta)

    #splitting valid uuid data based on similar types
    similar_events(valid_uuid_df)






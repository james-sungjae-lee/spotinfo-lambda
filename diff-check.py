from tqdm import tqdm
import pandas as pd
import argparse
import pickle
import boto3
import io

parser.add_argument('--bucket_name', default='', type=str,
                    help='name of s3 bucket')
parser.add_argument('--data_cond', default='', type=str,
                    help='condition of process data (2021/10/18)')

args = parser.parse_args()

bucket_name = args.bucket_name
data_cond = args.data_cond
s3 = boto3.resource('s3')
spotinfo_bucket = s3.Bucket(bucket_name)
object_list = [x.key for x in spotinfo_bucket.objects.all()]

def check_diff(before_df, after_df, region, instance):
    before_cond = ((before_df['Region'] == region) & (before_df['Instance Info'] == instance))
    after_cond = ((after_df['Region'] == region) & (after_df['Instance Info'] == instance))
    if len(before_df[before_cond]) == 0 and len(after_df[after_cond]) == 0:
        return
    elif len(before_df[before_cond]) == 0 and len(after_df[after_cond]) == 1:
        return ('new-instance', f'{region},{instance}')
    elif len(before_df[before_cond]) == 1 and len(after_df[after_cond]) == 0:
        return ('removed-instance', f'{region},{instance}')
    elif len(before_df[before_cond]) == 1 and len(after_df[after_cond]) == 1:
        before_freq = before_df[before_cond]['Frequency of interruption'].values[0]
        after_freq = after_df[after_cond]['Frequency of interruption'].values[0]
        before_cost = before_df[before_cond]['USD/Hour'].values[0]
        after_cost = after_df[after_cond]['USD/Hour'].values[0]
        
        if before_freq != after_freq:
            return ('freq-change', f'{region},{instance},{before_freq},{after_freq}')
        if before_cost != after_cost:
            return ('cost-change', f'{region},{instance},{before_cost},{after_cost}')
        return ('no-change', None)
    else:
        return ('Error (Too many rows)', f'{region},{instance},{len(before_df[before_cond])},{len(after_df[after_cond])}')
    
process_data = [x for x in object_list if data_cond in x]
change_dict = {}
for i in tqdm(range(len(process_data)-1)):
    before_file = process_data[i]
    after_file = process_data[i+1]
    change_dict[f'{before_file}-{after_file}'] = []

    before_df = pd.read_csv(f's3://{bucket_name}/{before_file}', skiprows=1)
    after_df = pd.read_csv(f's3://{bucket_name}/{after_file}', skiprows=1)
    region_list = before_df['Region'].unique().tolist()
    instance_list = before_df['Instance Info'].unique().tolist()

    for region in region_list:
        for instance in instance_list:
            change_data = check_diff(before_df, after_df, region, instance)
            if change_data != None:
                change_dict[f'{before_file}-{after_file}'].append(change_data)
                
for key, value in change_dict.items():
    print(key)
    print(pd.DataFrame(value)[0].value_counts())
    print()

pickle.dump(favorite_color, open(f'{cond_data}-out.p', 'wb'))

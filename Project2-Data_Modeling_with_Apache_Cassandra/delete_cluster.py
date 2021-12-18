#!/usr/bin/env python
# coding: utf-8


# # File to delete Redshift Cluster using the AWS python SDK (IAC)

import pandas as pd
import boto3
#import json

import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

PRO_CLUSTER_IDENTIFIER = config.get("PRO","PRO_CLUSTER_IDENTIFIER")
PRO_IAM_ROLE_NAME      = config.get("PRO", "PRO_IAM_ROLE_NAME")



def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def main():
    

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )
    
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    # Delete resources
    try:
        redshift.delete_cluster(ClusterIdentifier=PRO_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e:
        print("Cluster {} not found!".format(PRO_CLUSTER_IDENTIFIER))
    
    result = False
    
    while not result:
        try:
        # Check if the cluster has been deleted 
            ClusterProps = redshift.describe_clusters(ClusterIdentifier=PRO_CLUSTER_IDENTIFIER)['Clusters'][0]
            prettyRedshiftProps(ClusterProps)
            result = True
            print('Check cluster deletetion!!')
        except Exception as e:
            print("Cluster {} cannot be deleted because not found!".format(PRO_CLUSTER_IDENTIFIER))
            break
        
    try:    
        iam.detach_role_policy(RoleName=PRO_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=PRO_IAM_ROLE_NAME)
        print('IAM deleted')
    except Exception as e:
        print("IAM role {} cannot be deleted because not found!".format(PRO_IAM_ROLE_NAME))
    

if __name__ == "__main__":
    main()


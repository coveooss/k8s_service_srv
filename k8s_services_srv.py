import time
import boto3
import click
from kubernetes import config as k8s_config, client, watch
import logging
import urllib3
from urllib3.exceptions import ReadTimeoutError
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# Global variable
K8S_V1_CLIENT = client.CoreV1Api()
K8S_WATCHED_EVENTS = ["ADDED", "MODIFIED"]
REGION = ""
DOMAIN_NAME = ""
TTL = 3600
DBCLEANUP_FREQ = 300

# Constants
R53_RETRY = 10


def get_k8s_config():
    # Try to get k8s local config or use incluster one
    try:
        k8s_config.load_kube_config()
    except Exception as e:
        logging.info(
            'Error using local config ({}), try using kubernetes "In Cluster" config'.format(e))
        try:
            k8s_config.load_incluster_config()
        except Exception as e:
            raise(Exception('No k8s config suitable, exiting ({})'.format(e)))
    else:
        logging.info('Using Kubernetes local configuration')


def update_r53_serviceendpoints(srv_record_name, r53_zone_id, table):
    priority = 10
    response = table.scan()
    datas = response['Items']
    srv_record = []
    index = 0
    if len(datas) > 0:
        for data in datas:
            index += 1
            endpoint_dict = (data['endpoint']).split(':')
            srv_record.append({"Value": "{} {} {} {}".format(
                index, priority, endpoint_dict[1], endpoint_dict[0])})
    else:
        # No data in DynamoDB
        logging.info("No data in DynamoDB backend, skipping synchro")
        return True

    try:
        r53 = boto3.client('route53')
        response = r53.change_resource_record_sets(
            HostedZoneId=r53_zone_id,
            ChangeBatch={
                'Changes': [{
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': srv_record_name,
                        'Type': 'SRV',
                        'TTL': 300,
                        'ResourceRecords': srv_record
                    }}
                ]
            })
    except Exception as e:
        logging.error(
            'DNS record {} has not been updated : {}'.format(srv_record_name, e))
        raise(e)
    else:
        # If AWS route 53 Throtle the request, we return null and retry in the main loop
        if response['ResponseMetadata']['HTTPStatusCode'] == 400:
            logging.warning("route53 throttle on call {}".format(
                response['ResponseMetadata']['RequestId']))
            return False
        elif response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise(Exception("Error updating r53 DNS record {} (request ID : {}".format(
                srv_record_name, response['ResponseMetadata']['RequestId'])))
        else:
            logging.info('Updating DNS record {} ({})'.format(
                srv_record_name, response['ResponseMetadata']['RequestId']))
            return True


def upsert_dynamo_cluster_backend(cluster, endpoint, table):
    is_update = False
    bdd_value = "{}:{}".format(endpoint['server'], endpoint['port'])

    try:
        # Determine if it's a TTL update or not
        endpoints_in_backend = table.query(
            KeyConditionExpression=Key('endpoint').eq(bdd_value)
        )
        if endpoints_in_backend['Count'] > 0:
            is_update = True

        # Update or add the endpoint in the backend
        response = table.put_item(
            Item={
                'endpoint': bdd_value,
                'cluster': cluster,
                'last_seen': str(time.time())
            }
        )
    except Exception as e:
        raise(e)
    else:
        if is_update:
            logging.debug("Endpoint's TTL ({}) successfully updated in DynamoDB (ID : {})".format(
                bdd_value, response['ResponseMetadata']['RequestId']))
            return False
        else:
            logging.info("Endpoint {} successfully added to DynamoDB (ID : {})".format(
                bdd_value, response['ResponseMetadata']['RequestId']))
            return True


def del_dynamo_cluster_backend(cluster, endpoint, table):
    bdd_value = "{}:{}".format(endpoint['server'], endpoint['port'])
    try:
        response = table.delete_item(
            Key={
                'endpoint': bdd_value
            }
        )
    except ClientError as e:
        raise(e)
    else:
        logging.info("Endpoint {} successfully deleted from DynamoDB (ID : {})".format(
            bdd_value, response['ResponseMetadata']['RequestId']))
        return True


def clean_dynamo_cluster_backend(table):
    # Cleaning endpoints with expired TTL
    logging.info("Cleaning up the DB backend")
    response = table.scan()

    for item in response['Items']:

        # To manage migration from non TTL records
        if 'last_seen' not in item:
            item['last_seen'] = 0

        if time.time() - float(item['last_seen']) > TTL:
            endpoint_list = (item["endpoint"]).split(":")
            endpoint = {}
            endpoint['server'] = endpoint_list[0]
            endpoint['port'] = endpoint_list[1]
            del_dynamo_cluster_backend(
                item['cluster'], endpoint, table)


def get_k8s_services(name, namespace):
    # Get the k8s service with specific name
    services_list = []
    try:
        service = K8S_V1_CLIENT.read_namespaced_service(name=name, namespace=namespace)
        server = get_k8s_endpoint_node(name, namespace)
        if server:
            for port in service.spec.ports:
                services_list.append({
                    "server": server,
                    "port": port.node_port
                })
        return services_list
    except Exception as e:
        raise(Exception("Unexpected k8s API response : {}".format(e)))


def get_node_hostname(node_name):
    """
    Grab EC2 Tag "Name" and suffix it by the R53 domain name.
    :param node_name:
    :return: Dns instance Name as registered in R53
    """
    ec2_client = boto3.client('ec2', region_name=REGION)
    options = {"Filters": [
        {
            'Name': 'private-dns-name',
            'Values': [node_name]
        },
    ]}
    rsp = ec2_client.describe_instances(**options)
    if len(rsp['Reservations']) == 1:
        instance_name = next((tag["Value"] for tag in rsp['Reservations']
                              [0]['Instances'][0]['Tags'] if tag['Key'] == 'Name'))
    else:
        raise Exception("Node not found or more than one result retrieved")
    return "{}.{}".format(instance_name, DOMAIN_NAME)


def get_k8s_endpoint_node(name, namespace):
    # Get the node hosting the PODs
    try:
        node_name = K8S_V1_CLIENT.list_namespaced_endpoints(
            namespace=namespace, field_selector="metadata.name={}".format(name))
    except Exception as e:
        raise(Exception("Unexpected k8s API response : {}".format(e)))

    if len(node_name.items) > 1 or len(node_name.items) == 0:
        logging.error(
            "Unexpected k8s Endpoint response for endpoints matching name: {}".format(name))
        return ""
    else:
        try:
            return get_node_hostname(node_name.items[0].subsets[0].addresses[0].node_name)
        except Exception as e:
            logging.warning(
                "k8s endpoints have no target ({})".format(e))
            return ""


def create_dynamo_table(dynamodb_table_name, dynamodb_client):
    try:
        dynamodb_client.create_table(
            TableName=dynamodb_table_name,
            KeySchema=[
                {
                    'AttributeName': 'endpoint',
                    'KeyType': 'HASH'  # Partition key
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'cluster-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'cluster',
                            'KeyType': 'HASH'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'endpoint',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'cluster',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logging.info('Creating DynamoDB table {} ...'.format(
            dynamodb_table_name))
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=dynamodb_table_name)
        logging.info('DynamoDB table {} is created.'.format(
            dynamodb_table_name))
        return True
    except Exception as e:
        raise(e)


@click.command()
@click.option("--label_selector", default="", help="Specify the service labels to monitor")
@click.option("--namespace", default="kube-system", help="Specify the service labels to monitor")
@click.option("--srv_record", required=True, default=None, help="Specify DNS service record to update")
@click.option("--r53_zone_id", required=True, default=None, help="Specify route 53 DNS service record to update")
@click.option("--k8s_endpoint_name", required=False, default=None, help="Specify an alternative k8s endpoint name to store in r53 TXT record")
@click.option("--dynamodb_table_name", required=False, default="r53-service-resolver", help="Specify an alternative DynamoDB table name")
@click.option("--dynamodb_region", required=False, default="us-east-1", help="Region where the DynamoDB table is hosted")
@click.option("--region", "-r", required=False, default="us-east-1", help="AWS region")
@click.option("--log-level", type=click.Choice(["info", "debug", "warning", "error"], case_sensitive=True), required=False, default="info", help="Change log level")
def main(label_selector, namespace, srv_record, r53_zone_id, k8s_endpoint_name, dynamodb_table_name, dynamodb_region, region, log_level):

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=log_level.upper())

    global K8S_V1_CLIENT, REGION, DOMAIN_NAME, TTL

    REGION = region

    dynamodb_client = boto3.client('dynamodb', region_name=dynamodb_region)
    r53 = boto3.client('route53')
    DOMAIN_NAME = r53.get_hosted_zone(Id=r53_zone_id)["HostedZone"]["Name"]

    # Check if Dynamo DB Table exists
    try:
        logging.info("Connecting to DynamoDB table {}".format(
            dynamodb_table_name))
        dynamodb_client.describe_table(TableName=dynamodb_table_name)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        # If not, we create the table
        create_dynamo_table(dynamodb_table_name, dynamodb_client)

    dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
    dynamo_table = dynamodb.Table(dynamodb_table_name)

    try:
        get_k8s_config()
        K8S_V1_CLIENT = client.CoreV1Api()
        k8s_watch = watch.Watch()
        if not k8s_endpoint_name:
            api_endpoint_url = k8s_watch._api_client.configuration.host
            api_endpoint = api_endpoint_url.replace('https://', "")
        else:
            api_endpoint = k8s_endpoint_name

    except Exception as e:
        raise(Exception("Error connection k8s API {}".format(e)))

    while True:
        K8S_V1_CLIENT = client.CoreV1Api()
        k8s_watch = watch.Watch()
        logging.info("Watching k8s API for service change")
        try:
            stream = k8s_watch.stream(K8S_V1_CLIENT.list_namespaced_service, namespace=namespace,
                                      label_selector=label_selector, _request_timeout=DBCLEANUP_FREQ)
            for event in stream:
                logging.info('K8s service modification detected ({} : {})'.format(
                    event['type'], event['object']._metadata.name))
                if event['type'] in K8S_WATCHED_EVENTS:
                    service_k8s = {}
                    endpoints = get_k8s_services(event['object']._metadata.name, namespace)

                    for endpoint in endpoints:
                        backend_updated = upsert_dynamo_cluster_backend(
                            api_endpoint, endpoint, dynamo_table)

                    if backend_updated:
                        try:
                            retry_count = 0
                            # In case of r53 throttle, we wait for some time and try again
                            while not update_r53_serviceendpoints(srv_record, r53_zone_id, dynamo_table) and retry_count < R53_RETRY:
                                retry_count += 1
                                logging.info("Waiting for {} seconds".format(retry_count*retry_count))
                                time.sleep(retry_count*retry_count)
                                if retry_count >= R53_RETRY:
                                    raise(
                                        Exception("Error updating r53 info, exiting"))

                        except Exception as e:
                            raise(e)
                    else:
                        logging.info(
                            'K8s service modification detected but no DNS update is required')
                else:
                    logging.warning("Unmanaged event type : {}".format(event['type']))
                    logging.warning(event)
        except(ReadTimeoutError):

            # Remove old backend elements
            clean_dynamo_cluster_backend(dynamo_table)

            try:
                logging.info("Performing full sync between DynamoDB and route53")
                update_r53_serviceendpoints(srv_record, r53_zone_id, dynamo_table)
            except Exception as e:
                logging.warning(
                    "Full synchro failed between DynamoDB and route53")

        except Exception as e:
            raise(e)


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

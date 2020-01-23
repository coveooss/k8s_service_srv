import time
import boto3
import click
from kubernetes import config as k8s_config, client, watch
import logging
import urllib3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# Global variable
K8S_V1_CLIENT = client.CoreV1Api()
K8S_WATCHED_EVENTS = ["ADDED", "MODIFIED", "DELETED"]

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
    i = 0
    if len(datas) > 0:
        for data in datas:
            i += 1
            endpoint_dict = (data['endpoint']).split(':')
            srv_record.append({"Value": "{} {} {} {}".format(
                i, priority, endpoint_dict[1], endpoint_dict[0])})
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


def get_dynamo_cluster_services(k8s_endpoint, table):
    try:
        response = table.query(
            IndexName='cluster-index',
            KeyConditionExpression=Key('cluster').eq(k8s_endpoint)
        )
    except ClientError as e:
        raise(e)
    else:
        items = response['Items']
        cluster = []
        for item in items:
            endpoint_obj = (item['endpoint']).split(':')
            # TODO : intégrer la gestion du TTL
            cluster.append(
                {'server': endpoint_obj[0], 'port': int(endpoint_obj[1])})

        return({k8s_endpoint: cluster})


def add_dynamo_cluster_backend(cluster, endpoint, table):
    try:
        bdd_value = "{}:{}".format(endpoint['server'], endpoint['port'])
        response = table.put_item(
            Item={
                'endpoint': bdd_value,
                'cluster': cluster,
                'last_seen': time.time()
            }
        )
    except Exception as e:
        raise(e)
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


def list_k8s_services(namespace, label_selector):
    # Get the k8s service with specific labels
    services_list = []
    try:
        service = K8S_V1_CLIENT.list_namespaced_service(
            namespace=namespace, label_selector=label_selector)
        for item in service.items:
            server = get_k8s_endpoint_node(item.metadata.name, namespace)
            if server:
                for port in item._spec._ports:
                    services_list.append({
                        "server": server,
                        "port": port.node_port
                    })
        return services_list
    except Exception as e:
        raise(Exception("Unexpected k8s API response : {}".format(e)))


def get_k8s_endpoint_node(name, namespace):
    # Get the node hosting the PODs
    try:
        node_name = K8S_V1_CLIENT.list_namespaced_endpoints(
            namespace=namespace, field_selector="metadata.name={}".format(name))
    except Exception as e:
        raise(Exception("Unexpected k8s API response : {}".format(e)))

    if (len(node_name._items) > 1 or len(node_name._items) == 0):
        logging.error(
            "Unexpected k8s Endpoint response for endpoints matching name: {}".format(name))
        return ""
    else:
        try:
            return node_name._items[0]._subsets[0]._addresses[0].node_name
        except Exception as e:
            logging.warning(
                "k8s endpoints have no target ({})".format(e))
            return ""


def diff(listA, listB):
    return [i for i in listA if i not in listB]


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
@click.option("--dynamodb_region", "-r", default="us-east-1", help="Region where the DynamoDB table is hosted")
def main(label_selector, namespace, srv_record, r53_zone_id, k8s_endpoint_name, dynamodb_table_name, dynamodb_region):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    global K8S_V1_CLIENT

    dynamodb_client = boto3.client('dynamodb', region_name=dynamodb_region)

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
        w = watch.Watch()
        if not k8s_endpoint_name:
            api_endpoint_url = w._api_client.configuration.host
            api_endpoint = api_endpoint_url.replace('https://', "")
        else:
            api_endpoint = k8s_endpoint_name

    except Exception as e:
        raise(Exception("Error connection k8s API {}".format(e)))

    while True:
        logging.info("Watching k8s API for serice change")
        stream = w.stream(K8S_V1_CLIENT.list_namespaced_service,
                          namespace=namespace, label_selector=label_selector, _request_timeout=60)

        # Do an initial sync between DynamoDB and route53
        try:
            logging.info(
                "Performing full sync between DynamoDB and route53")
            update_r53_serviceendpoints(srv_record, r53_zone_id, dynamo_table)
        except Exception as e:
            logging.warning(
                "Full synchro failed between DynamoDB and route53")
        try:
            for event in stream:
                logging.info('K8s service modification detected ({} : {})'.format(
                    event['type'], event['object']._metadata.name))
                if event['type'] in K8S_WATCHED_EVENTS:
                    service_k8s = {}
                    endpoints = list_k8s_services(namespace, label_selector)
                    # If cluster have no valid erndpoint, we ignore it
                    if len(endpoints) > 0:
                        service_k8s[api_endpoint] = endpoints

                    # Collects cluster endpoints in the backend
                    service_backend = get_dynamo_cluster_services(
                        api_endpoint, dynamo_table)

                    backend_updated = False
                    # In k8s but not in backend -> append to backend
                    svc_to_add = diff(service_k8s[api_endpoint],
                                      service_backend[api_endpoint])
                    if svc_to_add:
                        for endpoint in svc_to_add:
                            backend_updated = add_dynamo_cluster_backend(
                                api_endpoint, endpoint, dynamo_table)

                    # In backend but not in k8s -> delete in backend
                    endpoint_to_delete = diff(
                        service_backend[api_endpoint], service_k8s[api_endpoint])
                    if endpoint_to_delete:
                        for endpoint in endpoint_to_delete:
                            backend_updated = del_dynamo_cluster_backend(
                                api_endpoint, endpoint, dynamo_table)
                    # If the backend have been updated, r53 sync is needed
                    if backend_updated:
                        try:
                            t = 0
                            # In case of r53 throttle, we wait for some time and try again
                            while not update_r53_serviceendpoints(srv_record, r53_zone_id, dynamo_table) and t < R53_RETRY:
                                t += 1
                                logging.info(
                                    "Waiting for {} seconds".format(t*t))
                                time.sleep(t*t)
                                if t >= R53_RETRY:
                                    raise(
                                        Exception("Error updating r53 info, exiting"))

                        except Exception as e:
                            raise(e)
                    else:
                        logging.info(
                            'K8s service modification detected but no DNS update is required')
                else:
                    logging.error(
                        "Unmanaged event type : {}".format(event['type']))
                    logging.error(event)
        except Exception as e:
            logging.info(e)


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

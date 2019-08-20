import boto3
import click
from kubernetes import config as k8s_config, client, watch
import logging
import urllib3
import urllib.request
import json
import base64
import bz2

# Global variable
K8S_V1_CLIENT = client.CoreV1Api()


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
            logging.error('No k8s config suitable, exiting ({})'.format(e))
            exit(-1)
    else:
        logging.info('Using Kubernetes local configuration')


def get_r53_services(dns_record, r53_zone_id):
    # Collect TXT record information for multi-cluster infos
    try:
        r53 = boto3.client('route53')
        responses = r53.list_resource_record_sets(
            HostedZoneId=r53_zone_id,
            StartRecordName=dns_record,
            StartRecordType='TXT',
        )
    except Exception as e:
        logging.error(
            'DNS record {} has not been found : {}'.format(dns_record, e))
        exit(-1)
    else:
        services = []
        # Parse all DNS from r53 answers
        for response in responses['ResourceRecordSets']:
            if response['Name'] == dns_record and response['Type'] == 'TXT':
                # Parse all values (for each cluster)
                for service in response['ResourceRecords']:
                    # remove trailing quotes
                    raw_value = (service['Value'])[1:-1]
                    # Convert raw compressed, base64 value to json
                    decoded_value = decode_b64(raw_value)
                    services.append(json.loads(decoded_value))
                return services


def encode_b64(value):
    # Compress and encode a string to avoid special characters
    return base64.urlsafe_b64encode(
        bz2.compress(
            value.encode('utf-8')
        )
    ).decode('utf-8')


def decode_b64(value):
    # Decompress and decode a string to avoid special characters
    return bz2.decompress(
        base64.urlsafe_b64decode(
            value.encode('utf-8')
        )
    ).decode('utf-8')


def update_r53_serviceendpoints(srv_record_name, api_endpoint, k8s_services, all_services, r53_zone_id):
    priority = 10
    srv_record = []
    txt_record = []

    i = 0

    for all_cluster_endpoints in all_services:
        if api_endpoint in all_cluster_endpoints.keys():
            services = k8s_services
        else:
            services = all_cluster_endpoints

        txt_record.append({"Value": '"{}"'.format(
            encode_b64(json.dumps(services)))})
        for endpoints in services.values():
            for endpoint in endpoints:
                i += 1
                srv_record.append({"Value": "{} {} {} {}".format(
                    i, priority, endpoint['port'], endpoint['server'])})
    print(txt_record)
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
                    }},
                    {
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': srv_record_name,
                        'Type': 'TXT',
                        'TTL': 300,
                        'ResourceRecords': txt_record
                    }}
                ]
            })

    except Exception as e:
        logging.error(
            'DNS record {} has not been updated : {}'.format(srv_record_name, e))
        print(e)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.error("Error updating r53 DNS record {} (request ID : {}".format(
                srv_record_name, response['ResponseMetadata']['RequestId']))
            exit(1)
        else:
            logging.info('Updating DNS record {} ({})'.format(
                srv_record_name, response['ResponseMetadata']['RequestId']))


def list_k8s_services(namespace, label_selector):
    services_list = []
    try:
        service = K8S_V1_CLIENT.list_namespaced_service(
            namespace=namespace, label_selector=label_selector)
        for item in service.items:
            server = get_k8s_endpoint_node(item.metadata.name, namespace)
            for port in item._spec._ports:
                services_list.append({
                    "server": server,
                    "port": port.node_port
                })
        return services_list
    except Exception as e:
        logging.error("Unexpected k8s API response : {}".format(e))
        exit(1)


def get_k8s_endpoint_node(name, namespace):
    try:
        node_name = K8S_V1_CLIENT.list_namespaced_endpoints(
            namespace=namespace, field_selector="metadata.name={}".format(name))
    except Exception as e:
        logging.error(
            "Unexpected k8s API response : {}".format(e))
        exit(1)
    if (len(node_name._items) > 1):
        logging.error(
            "Unexpected k8s Endpoint response, too many endpoints matching name: {}".format(name))
        exit(1)
    else:
        return node_name._items[0]._subsets[0]._addresses[0].node_name


@click.command()
@click.option("--region", "-r", default=None, help="Region where to run the script")
@click.option("--label_selector", default="", help="Specify the service labels to monitor")
@click.option("--namespace", default="kube-system", help="Specify the service labels to monitor")
@click.option("--srv_record", required=True, default=None, help="Specify DNS service record to update")
@click.option("--r53_zone_id", required=True, default=None, help="Specify route 53 DNS service record to update")
def main(region, label_selector, namespace, srv_record, r53_zone_id):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    global K8S_V1_CLIENT
    try:
        get_k8s_config()
        K8S_V1_CLIENT = client.CoreV1Api()
        w = watch.Watch()
        api_endpoint_url = w._api_client.configuration.host
        api_endpoint = api_endpoint_url.replace('https://', "")
    except Exception as e:
        logging.error("Error connection k8s API {}".format(e))
        exit(-1)

    logging.info("Watching k8s API for serice change")
    stream = w.stream(K8S_V1_CLIENT.list_namespaced_service,
                      namespace=namespace, label_selector=label_selector)
    for event in stream:
        logging.info('K8s service modification detected ({} : {})'.format(
            event['type'], event['object']._metadata.name))

        # Get services in k8s
        k8s_services = {}
        k8s_services[api_endpoint] = list_k8s_services(
            namespace, label_selector)

        # TODO Search cluster in Array + no endpoint sur k8s_get
        all_dns_values = get_r53_services(srv_record, r53_zone_id)
        logging.info("Values in route53 TXT record {} : {}".format(
            srv_record, all_dns_values))
        for dns_value in all_dns_values:
            if api_endpoint in dns_value.keys():
                dns_services = dns_value
                break
            else:
                dns_services = []

        if k8s_services != dns_services:
            logging.info('DNS modification needed {}'.format(
                dns_services))
            update_r53_serviceendpoints(
                srv_record, api_endpoint, k8s_services, all_dns_values, r53_zone_id)
        else:
            logging.info(
                'K8s service modification detected - no update required')


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

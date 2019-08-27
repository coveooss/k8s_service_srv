import time
import boto3
import click
from kubernetes import config as k8s_config, client, watch
import logging
import urllib3
import json
import base64
import gzip

# Global variable
K8S_V1_CLIENT = client.CoreV1Api()
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
        if responses['ResponseMetadata']['HTTPStatusCode'] == 200:
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
                        try:
                            services.append(json.loads(decoded_value))
                        except:
                            logging.warning(
                                "Unable to load TXT service into json")
                    return services
            return services
        elif responses['ResponseMetadata']['HTTPStatusCode'] == 400:
            # If AWS route 53 Throtle the request, we return null and retry in the main loop
            logging.warning("route53 throttle on call {}".format(
                responses['ResponseMetadata']['RequestId']))
            return None


def encode_b64(value):
    # Compress and encode a string to avoid special characters
    return base64.urlsafe_b64encode(
        gzip.compress(
            value.encode('utf-8')
        )
    ).decode('utf-8')


def decode_b64(value):
    # Decompress and decode a string to avoid special characters
    try:
        return gzip.decompress(
            base64.urlsafe_b64decode(
                value.encode('utf-8')
            )
        ).decode('utf-8')
    except Exception as e:
        logging.warning("Unable to decode {}".format(value))
        return None


def update_r53_serviceendpoints(srv_record_name, api_endpoint, k8s_services, all_services, r53_zone_id):
    priority = 10
    srv_record = []
    txt_record = []

    i = 0

    # If the DNS records a not yet created, we need to set it by default
    if len(all_services) == 0:
        all_services = [k8s_services]
    else:
        # If the cluster is not yet in the list of clusters, we add it the the list
        if next((item for item in all_services if item.keys() == api_endpoint), None) == None:
            all_services.append(k8s_services)

    # Parsing all clusters found in the TXT record, looking for the current cluster
    for all_cluster_endpoints in all_services:
        if api_endpoint in all_cluster_endpoints.keys():
            services = k8s_services
        else:
            services = all_cluster_endpoints
        # Generatin TXT and SRV values
        txt_record.append({"Value": '"{}"'.format(
            encode_b64(json.dumps(services)))})
        for endpoints in services.values():
            for endpoint in endpoints:
                i += 1
                srv_record.append({"Value": "{} {} {} {}".format(
                    i, priority, endpoint['port'], endpoint['server'])})
    try:
        # Updating both DNS records in one batch
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
        # If AWS route 53 Throtle the request, we return null and retry in the main loop
        if response['ResponseMetadata']['HTTPStatusCode'] == 400:
            logging.warning("route53 throttle on call {}".format(
                response['ResponseMetadata']['RequestId']))
            return None
        elif response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.error("Error updating r53 DNS record {} (request ID : {}".format(
                srv_record_name, response['ResponseMetadata']['RequestId']))
            exit(1)
        else:
            logging.info('Updating DNS record {} ({})'.format(
                srv_record_name, response['ResponseMetadata']['RequestId']))
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
        logging.error("Unexpected k8s API response : {}".format(e))
        exit(1)


def get_k8s_endpoint_node(name, namespace):
    # Get the node hosting the PODs
    try:
        node_name = K8S_V1_CLIENT.list_namespaced_endpoints(
            namespace=namespace, field_selector="metadata.name={}".format(name))
    except Exception as e:
        logging.error(
            "Unexpected k8s API response : {}".format(e))
        exit(1)
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


@click.command()
@click.option("--region", "-r", default=None, help="Region where to run the script")
@click.option("--label_selector", default="", help="Specify the service labels to monitor")
@click.option("--namespace", default="kube-system", help="Specify the service labels to monitor")
@click.option("--srv_record", required=True, default=None, help="Specify DNS service record to update")
@click.option("--r53_zone_id", required=True, default=None, help="Specify route 53 DNS service record to update")
@click.option("--k8s_endpoint_name", required=False, default=None, help="Specify an alternative k8s endpoint name to store in r53 TXT record")
def main(region, label_selector, namespace, srv_record, r53_zone_id, k8s_endpoint_name):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    global K8S_V1_CLIENT

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
        # Get all services for all clusters in the DNS TXT record
        all_dns_values = get_r53_services(srv_record, r53_zone_id)
        t = 0
        # In case of route53 throttle, we retry the DNS call
        while all_dns_values == None and t < R53_RETRY:
            t += 1
            all_dns_values = get_r53_services(srv_record, r53_zone_id)
            if all_dns_values == None:
                logging.info("Waiting for {} seconds".format(t*t))
                time.sleep(t*t)
            if t >= R53_RETRY:
                logging.error("Error getting r53 info, exiting")
                exit(-1)

        logging.info("Values in route53 TXT record {} : {}".format(
            srv_record, all_dns_values))

        # Get the current cluster values
        dns_services = {}
        for dns_value in all_dns_values:
            if api_endpoint in dns_value.keys():
                dns_services = dns_value
                break
        # If DNS modification is needed
        if k8s_services != dns_services:
            logging.info('DNS modification needed {} -> {}'.format(
                dns_services, k8s_services))
            updated = update_r53_serviceendpoints(
                srv_record, api_endpoint, k8s_services, all_dns_values, r53_zone_id)
            # In case of route53 throttle, we retry the DNS call
            t = 0
            while updated == None and t < R53_RETRY:
                t += 1
                updated = update_r53_serviceendpoints(
                    srv_record, api_endpoint, k8s_services, all_dns_values, r53_zone_id)
                if updated == None:
                    logging.info("Waiting for {} seconds".format(t*t))
                    time.sleep(t*t)
                if t >= R53_RETRY:
                    logging.error("Error updating r53 info, exiting")
                    exit(-1)
        else:
            logging.info(
                'K8s service modification detected but no DNS update is required')


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

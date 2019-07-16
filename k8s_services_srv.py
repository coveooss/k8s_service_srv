import boto3
import click
from kubernetes import config as k8s_config, client, watch
import logging
import urllib.request
import json
import dns.resolver
import urllib3

# Global variable
K8S_V1_CLIENT = client.CoreV1Api()


def get_k8s_config():
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


def get_dns_value(record, dns_type):
    response_dict = {}
    response_list = []
    try:
        inf = dns.resolver.query(record, dns_type)
        for _resp in inf:
            response_list.append(_resp.to_text())
    except Exception as e:
        logging.error('Error resolving DNS record {} : {}'.format(record, e))
        exit(1)
    response_dict['answer'] = response_list
    return response_dict


def update_dns_services(srv_record, k8s_services, r53_zone_id):
    priority = 10
    services = []

    i = 1
    for service in k8s_services:
        services.append({"Value": "{} {} {} {}".format(
            i, priority, service['port'], service['server'])})
    try:
        r53 = boto3.client('route53')
        response = r53.change_resource_record_sets(
            HostedZoneId=r53_zone_id,
            ChangeBatch={
                'Changes': [{
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': srv_record,
                        'Type': 'SRV',
                        'TTL': 300,
                        'ResourceRecords': services
                    }}]
            })
    except Exception as e:
        logging.error(
            'DNS record {} has not been updated : {}'.format(srv_record, e))
        print(e)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logging.error("Error updating r53 DNS record {} (request ID : {}".format(
                srv_record, response['ResponseMetadata']['RequestId']))
            exit(1)
        else:
            logging.info('Updating DNS record {} ({})'.format(
                srv_record, response['ResponseMetadata']['RequestId']))


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


def list_dns_services(srv_record):
    raws = get_dns_value(srv_record, "SRV")['answer']
    services_list = []
    for raw in raws:
        dns_values = raw.split()
        record = {
            "server": dns_values[3][:-1],
            "port": int(dns_values[2])
        }
        services_list.append(record)
    return services_list


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
    except Exception as e:
        logging.error("Error connection k8s API {}".format(e))
        exit(-1)

    logging.info("Watching k8s API for serice change")
    stream = w.stream(K8S_V1_CLIENT.list_namespaced_service,
                      namespace=namespace, label_selector=label_selector)
    for event in stream:
        logging.info(
            'K8s service modification detected ({} : {})'.format(event['type'], event['object']._metadata.name))
        k8s_services = list_k8s_services(namespace, label_selector)
        dns_services = list_dns_services(srv_record)
        # Todo : need improvement when starting to watch API (all services are added)
        if k8s_services != dns_services:
            logging.info('DNS modification needed {}'.format(dns_services))
            update_dns_services(srv_record, k8s_services, r53_zone_id)
        else:
            logging.info(
                'K8s service modification detected - no update required')


if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()

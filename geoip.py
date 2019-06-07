import requests
from copy import deepcopy
import util


GEO_IP_URL = 'http://api.ipstack.com/{0}?access_key=5635018d1ae4fe81a3e4ed450ed62bfe'


def get(ip, process=True):
    """
    Get GeoIP data for an IP.
    :param ip:
    :param process: whether or not to process the GeoIP data or leave it raw
    :return: GeoIP data
    """
    util.verify_ip(ip)

    response = requests.get(GEO_IP_URL.format(ip))

    data = response.json()
    if process:
        data = process_data(data)

    return data


def get_all(ips, process=True):
    """
    Get GeoIP data for multiple IPs.
    :param ips: list of IPs to get GeoIP data for
    :param process: whether or not to process the GeoIP data or leave it raw
    :return: list of GeoIP info objects
    """
    data = []

    for ip in ips:
        data.append(get(ip, process))

    return data


def process_data(data):
    """
    Process GeoIP data.
    :param data: the data to process
    :return: the processed data
    """
    if not isinstance(data, dict):
        raise Exception('Value is not a dict. Type: {0}'.format(type(data)))

    new_data = deepcopy(data)
    del new_data['location']
    return new_data

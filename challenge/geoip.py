import requests
from copy import deepcopy
from challenge import util

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
        data = _process_data(data)

    return data


def _process_data(data):
    """
    Process GeoIP data.
    :param data: the data to process
    :return: the processed data
    """
    if not isinstance(data, dict):
        raise Exception('Value is not a dict. Type: {0}'.format(type(data)))

    new_data = deepcopy(data)

    if 'location' in new_data:
        del new_data['location']

    return new_data


def get_key(data):
    return data['ip']

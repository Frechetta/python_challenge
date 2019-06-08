import re
from challenge import geoip, rdap

IP_PATTERN = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')


def verify_ip(val):
    """
    Verify a value represents an IP address.
    Raise an exception if the value is not a IP address.
    :param val:
    :return: None
    """
    if not isinstance(val, str):
        raise Exception(f'Value is not a string. Type: {type(val)}')

    if not IP_PATTERN.fullmatch(val):
        raise Exception('Value does not seem to be an IPv4 address')


def get_key_func(index):
    """
    Get the key function for the specified index.
    :param index:
    :return: they key function
    """
    if index == 'geoip':
        return geoip.get_key
    if index == 'rdap':
        return rdap.get_key
    if index == 'ip_rdap':
        return lambda event: '{0}:{1}'.format(event['ip'], event['handle'])

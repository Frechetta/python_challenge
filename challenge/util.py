import re
from challenge import geoip, rdap

IP_PATTERN = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')


def verify_ip(ip):
    if not isinstance(ip, str):
        raise Exception('Value is not a string. Type: {0}'.format(type(ip)))

    if not IP_PATTERN.fullmatch(ip):
        raise Exception('Value does not seem to be an IPv4 address'.format(ip))


def get_key_func(index):
    if index == 'geoip':
        return geoip.get_key
    if index == 'rdap':
        return rdap.get_key

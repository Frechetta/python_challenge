import re

IP_PATTERN = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')


def verify_ip(ip):
    if not isinstance(ip, str):
        raise Exception('Value is not a string. Type: {0}'.format(type(ip)))

    if not IP_PATTERN.fullmatch(ip):
        raise Exception('Value does not seem to be an IPv4 address'.format(ip))

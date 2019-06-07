from challenge import util


def read_ips(path):
    """
    Read a text file and pull out IPs.
    :param path: the path to the text file
    :return: dictionary of IPs and their counts
    """
    ips = {}

    with open(path) as file:
        for line in file:
            line_ips = util.IP_PATTERN.findall(line)
            for ip in line_ips:
                if ip not in ips:
                    ips[ip] = 1
                else:
                    ips[ip] += 1

    return ips

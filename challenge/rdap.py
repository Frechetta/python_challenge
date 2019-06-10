import requests
from challenge import util

RDAP_URL = 'https://rdap.arin.net/registry/ip/{0}'

INTERESTING_TOP_LEVEL_FIELDS = ['handle', 'startAddress', 'endAddress', 'ipVersion', 'name', 'type', 'parentHandle', 'objectClassName']


def get(ip, process=True):
    """
    Perform an RDAP query against an IP.
    Response format is described here: https://tools.ietf.org/html/rfc7483.
    :param ip:
    :param process: whether or not to process the GeoIP data or leave it raw
    :return: RDAP data
    """
    util.verify_ip(ip)

    response = requests.get(RDAP_URL.format(ip))

    if response.status_code == 404:
        return None

    result = response.json()

    if process:
        result = _process_data(result)

    return result


def _process_data(data):
    """
    Process RDAP data.
    Returns an array where one item is for top-level RDAP data, and items for each child entity.
    :param data: the data to process
    :return: processed data in the form of an array
    """
    if not isinstance(data, dict):
        raise Exception(f'Value is not a dict. Type: {type(data)}')

    top_level_data = {'class': 'root'}

    for field in INTERESTING_TOP_LEVEL_FIELDS:
        if field in data:
            top_level_data[field] = data[field]

    if 'events' in data:
        top_level_data.update(_parse_events(data['events']))

    if 'status' in data:
        top_level_data['status'] = ','.join(data['status'])

    all_data = [top_level_data]

    if 'entities' in data:
        entities = filter(lambda e: 'objectClassName' in e and e['objectClassName'] == 'entity', data['entities'])
        for entity in entities:
            all_data += _parse_entity(entity, top_level_data['handle'])

    return all_data


def _parse_events(events):
    """
    Parse RDAP events.
    :param events:
    :return: parsed events
    """
    parsed_events = {}

    for event in events:
        if 'eventAction' not in event or 'eventDate' not in event:
            continue

        action = event['eventAction'].replace(' ', '_')
        key = f'event_{action}'

        parsed_events[key] = event['eventDate']

    return parsed_events


def _parse_entity(entity, parent):
    """
    Parse RDAP entities.
    An entity can have child entities, therefore this function returns an array; one item for the top-level entity
    and an item for each child.
    :param entity:
    :param parent: parent of the entity
    :return: list of parsed entities.
    """
    top_level_entity = {'class': 'child', 'parentHandle': parent}

    if 'handle' in entity:
        top_level_entity['handle'] = entity['handle']

    if 'vcardArray' in entity:
        vcard_items = entity['vcardArray'][1]
        top_level_entity.update(_parse_vcard(vcard_items))

    if 'roles' in entity:
        top_level_entity['roles'] = ','.join(entity['roles'])

    if 'events' in entity:
        top_level_entity.update(_parse_events(entity['events']))

    entities = [top_level_entity]

    if 'entities' in entity:
        for entity in entity['entities']:
            entities += _parse_entity(entity, top_level_entity['handle'])

    return entities


def _parse_vcard(vcard):
    """
    Parse a vCard from an RDAP entity.
    :param vcard:
    :return: the parsed vCard
    """
    parsed_vcard = {}

    for item in vcard:
        key = item[0]

        if key == 'adr':
            if 'label' in item[1]:
                value = item[1]['label']
            else:
                value = '\n'.join(item[3])
        else:
            value = item[3]
            if isinstance(value, list):
                value = ','.join(value)

        key = f'vcard_{key}'

        parsed_vcard[key] = value

    return parsed_vcard


def get_key(data):
    """
    Produce a key for the specified data.
    :param data:
    :return: the key
    """
    return data['handle']

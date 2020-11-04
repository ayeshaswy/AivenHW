from jproperties import Properties


"""
References: https://www.journaldev.com/39861/python-read-properties-file
"""

def load_properties(properties_file):
    """
    Loads the properties from the supplied file.

    :param properties_file: The file to load the properties from.
    :return: Dictionary with property names as keys and their corresponding value as values.
    """
    properties = Properties()

    db_configs_dict = {}

    with open(properties_file, 'rb') as prop_file:
        properties.load(prop_file)

    items_view = properties.items()

    for item in items_view:
        db_configs_dict[item[0]] = item[1].data

    # converting conventional version format to the format expected byt Kafka-python Consumer/Producer APIs.
    if 'api_version' in db_configs_dict:
        db_configs_dict['api_version'] = tuple(int(x) for x in db_configs_dict['api_version'].split('.'))

    return db_configs_dict



from json_reader import load_json
import pandas as pd

"""
The mappings are of the shape key:value where the key is the actual value and value is the anonymized value
"""

def map_json_to_table(path):

    og = load_json(path)  # This is a bad practice and should be fixed to a spark version. It will work cause the mappings are small.

    data = {
        'name': list(),
        'anonymized': list()
    }

    for k,v in og.items():
        data['name'].append(k)
        data['anonymized'].append(v)

    df = pd.DataFrame(data)
    return spark.createDataFrame(df)
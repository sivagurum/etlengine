import json
import yaml

with open("/Users/jagadeeshkumarsellappan/Documents/imdb.yaml") as f:
    yaml_data = yaml.load(f)

with open("/Users/jagadeeshkumarsellappan/Documents/imdb.json", 'w') as outfile:
    json.dump(yaml_data, outfile)
import csv
import re
import requests
import yaml
from tqdm import tqdm
import gzip

# CURIE mappings, in descending order of priority
sources = [
    ['dipper', 'https://raw.githubusercontent.com/monarch-initiative/dipper/master/dipper/curie_map.yaml'],
    ['biolink', 'https://raw.githubusercontent.com/biolink/biolink-model/master/biolink-model.yaml']]

mapping = []
for m in tqdm(sources, "making map"):
    y = yaml.safe_load(requests.get(m[1]).text)
    if m[0] == 'biolink':
        y = y['prefixes']
    mapping.append({k: v for k, v in y.items()})


def curie_to_url(curie) -> str:
    if re.match("^[^:]+:[^:]+$", line[0]):
        prefix, body = curie.split(":")
        for m in mapping:
            if prefix in m:
                return m[prefix] + body
    return curie


with gzip.open('merged-kg_nodes.tsv.gz', 'rt') as f:
    rd = csv.reader(f, delimiter="\t")
    header = next(rd)
    for line in tqdm(rd, "converting curies to urls"):
        print(curie_to_url(line[0]))


import pickle
import pandas as pd
from merge_label_pickles import *
from tqdm import tqdm, tqdm_pandas
tqdm.pandas()

if __name__ == '__main__':
    
    json_file =''
    subset_dirpath = ''
    required_lang = ['en', 'zh']

    id2label = load_from_json_file(json_file)

    def map_id_to_labels(x):
        id=x['wikidataID']
        x=x.drop(labels='label')
        for lang in required_lang:
            id ['en_label'] = id2label.get(lang)
        return x
        
    files = ['entities.tsv', 'nodes.tsv', 'relations.tsv']
    for file in files:
        df = pd.read_csv(file, sep='\t')
        df = df.progress_apply(map_id_to_labels, axis=1)
        df.to_csv(files, index=False)

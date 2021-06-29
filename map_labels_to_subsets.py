import pickle
import pandas as pd
from merge_label_pickles import *
from tqdm import tqdm, tqdm_pandas
tqdm.pandas()
import os
from multiprocessing import Pool

if __name__ == '__main__':
    
    pickle_dir ='labels_subset3/'
    subset_dirpath = ''
    required_lang = ['en', 'zh-hans']
    subdata_paths = ['companies']

    id2label = load_from_pickle_dir(pickle_dir)

    def map_id_to_labels(x):
        id=x['wikidataID']
        try:
            for lang in required_lang:
                x[lang+'_label'] = id2label[id].get(lang)
            x=x.drop(labels='label')
        except KeyError:
            x['default_label']=x['label']
            x=x.drop(labels='label')
        return x
    
    def worker(file):
        print(f'Processor ID: {os.getpid()}')
        df = pd.read_csv(file, sep='\t')
        df = df.progress_apply(map_id_to_labels, axis=1)
        df.to_csv(file, index=False)
        print(f'Saved {file}.')

    files = ['entities.tsv', 'nodes.tsv', 'relations.tsv']
    all_files = []
    for subdata_path in subdata_paths:
        for file in files:
            file = os.path.join(subdata_path, file)
            all_files.append(file)
    
    pool = Pool(processes=3)
    pool.map(worker, all_files)
    print('ALL FINISHED.')
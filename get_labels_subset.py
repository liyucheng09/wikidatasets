import pickle
from tqdm import tqdm
from merge_label_pickles import *


if __name__ == '__main__':

    required_langs = ['en', 'fr', 'de', 'ja', 'zh-hans', 'ko', 'th', 'vi']
    pickle_dir = 'parsed_3/pickles'
#     pickle_dir2 = 'parsed_4/pickles'
    json_output_path = 'parsed_3/labels_subset.json'
    pickle_output_path = 'labels_subset/'

    id2label = load_from_pickle_dir(pickle_dir)
#     id2label_ = load_from_pickle_dir(pickle_dir2)
#     id2label.update(id2label_)
    
#     del id2label_

    lang_subset={}
    for id, labels in tqdm(id2label.items()):
        try:
            sub={lang: labels.get(lang) for lang in required_langs}
        except:
            sub = f'No labels for {id}.'
            continue
        lang_subset[id] = sub
    
    del id2label
    save_to_pickle(pickle_output_path, lang_subset)    
#     save_to_json(json_output_path, lang_subset)

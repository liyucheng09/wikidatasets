import pickle
from tqdm import tqdm
from merge_label_pickles import *


if __name__ == '__main__':

    required_langs = ['en', 'fr', 'de', 'ja', 'zh-hans', 'ko', 'th', 'vi']
    pickle_dir = 'all_labels/'
    json_output_path = 'parsed_3/labels_subset.json'
    pickle_output_path = 'labels_subset3/'

    id2label = load_from_pickle_dir(pickle_dir)

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

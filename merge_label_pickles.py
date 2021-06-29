import pickle
import os
import json
from tqdm import tqdm

def save_to_json(json_output_path, id2label):
    with open(json_output_path, 'w', encoding='utf-8') as f:
        for id, labels in tqdm(id2label.items()):
            dump_line = {'id':id, 'labels':labels}
            f.write(json.dumps(dump_line, ensure_ascii=False)+'\n')
    print(f'Sucessfully Saved to {json_output_path}.')

def save_to_pickle(pickle_output_path, id2label, num_lines=5000000):
    print('Start Pickling...')
    counter=0
    n_pickles = 0
    sub = {}
    for id in tqdm(id2label.keys()):
        counter+=1
        sub[id]=id2label[id]
        if counter % num_lines == 0:
            with open(pickle_output_path+f'labels_dump_{n_pickles}.pkl', 'wb') as f:
                pickle.dump(sub, f)
            n_pickles+=1
            sub={}
            print(f'Pickled {n_pickles} dump.')
    print(f'Sucessfully Saved to {pickle_output_path}.')

def load_from_pickle_dir(pickle_dir):
    files = os.listdir(pickle_dir)
    id2label = {}
    for file in tqdm(files):
        file_path = os.path.join(pickle_dir, file)
        with open(file_path, 'rb') as f:
            id2label.update(pickle.load(f))
    return id2label

def update_from_pickle_dir(pickle_dir, id2label):
    files = os.listdir(pickle_dir)
    for file in tqdm(files):
        file_path = os.path.join(pickle_dir, file)
        with open(file_path, 'rb') as f:
            id2label.update(pickle.load(f))
    return id2label

def load_from_pickle_file(pickle_file):
    id2label = {}
    with open(pickle_file, 'rb') as f:
        id2label.update(pickle.load(f))
    return id2label

def load_from_json_file(json_file):
    counter = 0
    id2label ={}
    with open(json_file) as f:
        while True:
            line = f.readline()
            if len(line) == 0:
                break
            counter +=1
            print('Processing [%d%%]\r'%counter, end="")
            line = json.loads(line)
            id2label[line['id']] = line['labels']
    return id2label

def progressively_get_labels_subset(input_pickle_dir, output_pickle_dir, required_langs):
    files = os.listdir(input_pickle_dir)
    for file in tqdm(files):
        file_path = os.path.join(input_pickle_dir, file)
        output_file_path = os.path.join(output_pickle_dir, file)
        with open(file_path, 'rb') as f:
            id2label=pickle.load(f)
            
        lang_subset={}
        for id, labels in tqdm(id2label.items()):
            try:
                sub={lang: labels.get(lang) for lang in required_langs}
            except:
                sub = f'No labels for {id}.'
                continue
            lang_subset[id] = sub
        with open(output_file_path, 'wb') as f:
            pickle.dump(lang_subset, f)
        lang_subset={}
        print(f'Successfully save to {output_file_path}')
    print('Finish subsetting all.')


if __name__ == '__main__':

    write_labels_to_json = True
    write_labels_to_pickle = True

    pickle_dir = 'parsed_3/pickles'
    pickle_output_path = 'parsed_3/all_labels.pkl'
    json_output_path = 'parsed_3/all_labels.txt'

    id2label = load_from_pickle_dir(pickle_dir)

    if write_labels_to_json:
        save_to_json(json_output_path)

    if write_labels_to_pickle:
        save_to_pickle(pickle_output_path)






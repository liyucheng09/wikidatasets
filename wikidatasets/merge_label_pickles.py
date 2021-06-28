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

def save_to_pickle(pickle_output_path, id2label):
    print('Start Pickling...')
    with open(pickle_output_path, 'wb') as f:
        pickle.dump(id2label, f)
    print(f'Sucessfully Saved to {pickle_output_path}.')

def load_from_pickle_dir(pickle_dir):
    files = os.listdir(pickle_dir)
    id2label = {}
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






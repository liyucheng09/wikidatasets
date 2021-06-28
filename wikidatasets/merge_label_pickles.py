import pickle
import os
import json
import tqdm

write_labels_to_json = True
write_labels_to_pickle = True

pickle_dir = 'parsed_3/pickles'
pickle_output_path = 'parsed_3/all_labels.pkl'
json_output_path = 'parsed_3/all_labels.txt'

files = os.listdir(pickle_dir)
id2label = {}
for file in files:
    file_path = os.path.join(pickle_dir, file)
    with open(file_path, 'rb') as f:
        id2label.update(pickle.load(f))

if write_labels_to_json:
    with open(json_output_path, 'w', encoding='utf-8') as f:
        for id, labels in tqdm(id2label.items()):
            dump_line = {'id':id, 'labels':labels}
            f.write(json.dumps(dump_line, ensure_ascii=False)+'\n')

if write_labels_to_pickle:
    print('Start Pickling...')
    with open(pickle_output_path, 'wb') as f:
        pickle.dump(id2label, f)






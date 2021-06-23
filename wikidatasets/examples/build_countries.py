import pickle
from wikidatasets.processFunctions import get_subclasses, query_wikidata_dump, build_dataset

# change the 4 following values to match your installation
path = '../countries/'  # this will contain the files output through the process
dump_path = 'latest-all.json.bz2'  # path to the bz2 dump file
n_lines = 81933324  # this can be an upper bound
test_entities = get_subclasses('Q6256')  # sequence of images that give the impression of movement

query_wikidata_dump(dump_path, path, n_lines, test_entities=test_entities, collect_labels=False)

labels = pickle.load(open(path + 'labels.pkl', 'rb'))
build_dataset(path, labels, dump_date='April 15, 2020')

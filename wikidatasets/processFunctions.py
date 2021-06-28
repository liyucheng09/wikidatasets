import bz2
import pickle
import os
import pandas as pd

from tqdm import tqdm
from wikidatasets.utils import get_results, clean
from wikidatasets.utils import get_pickle_path, write_to_pickle
from wikidatasets.utils import get_id, get_label, to_triplets, intersect, to_json, get_multiligual_labels
from wikidatasets.utils import concatpkls, write_csv, write_ent_dict, write_rel_dict, write_readme, relabel, multi_lingual_relabel


def get_subclasses(subject):
    """Get a list of WikiData IDs of entities which are subclasses of the subject.

    Parameters
    ----------
    subject: str
        String describing the subject (e.g. 'Q5' for human).

    Returns
    -------
    result: list
        List of WikiData IDs of entities which are subclasses of the subject.

    """
    endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT ?item WHERE {?item wdt:P279* wd:""" + subject + """ .}"""

    results = get_results(endpoint_url, query)

    return [clean(result['item']['value']) for result in results['results']['bindings']]


def query_wikidata_dump(dump_path, path, n_lines, test_entities=None, collect_labels=False, multi_lingual=False, skip_lines=None):
    """This function goes through a Wikidata dump. It can either collect entities that are instances of \
    `test_entities` or collect the dictionary of labels. It can also do both.

    Parameters
    ----------
    dump_path: str
        Path to the latest-all.json.bz2 file downloaded from https://dumps.wikimedia.org/wikidatawiki/entities/.
    path: str
        Path to where pickle files will be written.
    n_lines: int
        Number of lines of the dump. Fastest way I found was `$ bzgrep -c ".*" latest-all.json.bz2`.
        This can be an upper-bound as it is only used for displaying a progress bar.
    test_entities: list
        List of entities to check if a line is instance of. For each line (entity), we check if it as a fact of the \
        type (id, query_rel, test_entity).
    collect_labels: bool
        Boolean indicating whether the labels dictionary should be collected.
    multi_lingual: bool
        whether or not to collect multi-lingual labels for each node.
    skip_lines: int
        This is useful when resuming parsing, will skip the first skip_lines lines.

    """
    pickle_path = get_pickle_path(path)
    collect_facts = (test_entities is not None)
    fails = []

    n_pickle_dump = 0
    if collect_labels:
        labels = {}
    if collect_facts:
        facts = []

    ids = set()
    dump = bz2.open(dump_path, 'rt')
    progress_bar = tqdm(total=n_lines)
    counter = 0  # counter of the number of lines read
    line = dump.readline()  # the first line of the file should be "[\n" so we skip it

    while True:
        # while there are lines to read
        line = dump.readline().strip()
        if len(line) == 0:
            break

        counter += 1
        if skip_lines is not None:
            if counter < skip_lines - 1:
                continue
        progress_bar.update(1)

        try:
            line = to_json(line)

            if collect_labels:
                id_ = get_id(line)
                if id_ in ids:
                    continue
                ids.add(id_)
                if multi_lingual:
                    labels[id_] = get_multiligual_labels(line)
                else:
                    labels[id_] = get_label(line)

            if collect_facts:
                triplets, instanceOf = to_triplets(line)
                if len(instanceOf) > 0 and intersect(instanceOf, test_entities):
                    facts.extend(triplets)

        except:
            if type(line) == dict and ('claims' in line.keys()):
                if len(line['claims']) != 0:
                    fails.append(line)
            else:
                fails.append(line)

        if counter % 3000000 == 0:
            # dump in pickle to free memory
            n_pickle_dump += 1
            if collect_facts:
                facts, fails = write_to_pickle(pickle_path, facts, fails, n_pickle_dump)
            if collect_labels:
                pickle.dump(labels, open(pickle_path+f'labels_dump{n_pickle_dump}.pkl', 'wb'))
                print(f'Pickle Labels Number {n_pickle_dump}')
                labels={}

    n_pickle_dump +=1
    if collect_facts:
        _, _ = write_to_pickle(pickle_path, facts, fails, n_pickle_dump)
    if collect_labels:
        pickle.dump(labels, open(pickle_path + f'labels_dump{n_pickle_dump}.pkl', 'wb'))
        
def query_wikidata_dump_with_multi_processing(dump_path, path, n_lines, test_entities=None, collect_labels=False, multi_lingual=False, skip_lines=None, num_procs=4, size_of_queue=50000, memory_lines=3000000):
    """This function goes through a Wikidata dump. It can either collect entities that are instances of \
    `test_entities` or collect the dictionary of labels. It can also do both.
    
    This func apply multiprocessing.

    Parameters
    ----------
    dump_path: str
        Path to the latest-all.json.bz2 file downloaded from https://dumps.wikimedia.org/wikidatawiki/entities/.
    path: str
        Path to where pickle files will be written.
    n_lines: int
        Number of lines of the dump. Fastest way I found was `$ bzgrep -c ".*" latest-all.json.bz2`.
        This can be an upper-bound as it is only used for displaying a progress bar.
    test_entities: list
        List of entities to check if a line is instance of. For each line (entity), we check if it as a fact of the \
        type (id, query_rel, test_entity).
    collect_labels: bool
        Boolean indicating whether the labels dictionary should be collected.
    multi_lingual: bool
        whether or not to collect multi-lingual labels for each node.
    skip_lines: int
        This is useful when resuming parsing, will skip the first skip_lines lines.
    num_procs: int
        number of consumers processes.

    """
    from multiprocessing import Process, Queue
    
    pickle_path = get_pickle_path(path)
    collect_facts = (test_entities is not None)
    save_steps = int(memory_lines/num_procs)
    q=Queue(size_of_queue)
    ids = set()

    def producer_func(num_worker):
        dump = bz2.open(dump_path, 'rt')
        progress_bar = tqdm(total=n_lines)
        counter = 0  # counter of the number of lines read
        line = dump.readline()  # the first line of the file should be "[\n" so we skip it
        counter=0

        while True:
            # while there are lines to read
            line = dump.readline().strip()
            if len(line) == 0:
                for i in range(n_worker):
                    q.put('STOP')
                

            counter += 1
            progress_bar.update(1)

            if skip_lines is not None:
                if counter < skip_lines+1:
                    continue
            
            q.put(line)

    def consumer_func():
        process_id = os.getpid()
        print('process id:', process_id)
        fails = []

        if collect_labels:
            labels = {}
        if collect_facts:
            facts = []
            
        n_pickle_dump = 0
        counter=0
        
        while True:
            try:
                line=q.get()
            except:
                print(f'Process num {process_id} Error!')
                break
                
            counter+=1
            if line == 'STOP':
                break
        
            try:
                line = to_json(line)

                if collect_labels:
                    id_ = get_id(line)
                    if id_ in ids:
                        continue
                    ids.add(id_)
                    if multi_lingual:
                        labels[id_] = get_multiligual_labels(line)
                    else:
                        labels[id_] = get_label(line)

                if collect_facts:
                    triplets, instanceOf = to_triplets(line)
                    if len(instanceOf) > 0 and intersect(instanceOf, test_entities):
                        facts.extend(triplets)

            except:
                if type(line) == dict and ('claims' in line.keys()):
                    if len(line['claims']) != 0:
                        fails.append(line)
                else:
                    fails.append(line)

            if counter % save_steps == 0:
                # dump in pickle to free memory
                n_pickle_dump += 1
                suffix = '_' + str(process_id) + '_' + str(n_pickle_dump)
                if collect_facts:
                    facts, fails = write_to_pickle(pickle_path, facts, fails, suffix)
                if collect_labels:
                    pickle.dump(labels, open(pickle_path+f'labels_dump{suffix}.pkl', 'wb'))
                    print(f'Pickle Labels Number {suffix}')
                    labels={}

        n_pickle_dump +=1
        suffix = '_' + str(process_id) + '_' + str(n_pickle_dump)

        if collect_facts:
            _, _ = write_to_pickle(pickle_path, facts, fails, suffix)
        if collect_labels:
            pickle.dump(labels, open(pickle_path + f'labels_dump{suffix}.pkl', 'wb'))

    producer = Process(target=producer_func, args=(num_procs,))
    producer.daemon=True
    producer.start()
    
    consumers=[]
    for i in range(num_procs):
        consumer=Process(target=consumer_func)
        consumers.append(consumer)
        consumer.daemon=True
        consumer.start()
    
    for consumer in consumers:
        consumer.join()
    
    print('Finish ALL !')

def build_dataset(path, labels, return_=False, dump_date='23rd April 2019', multi_lingual=None):
    """Builds datasets from the pickle files produced by the query_wikidata_dump.

    Parameters
    ----------
    path: str
        Path to the directory where there should already be a pickles/ directory. In the latter directory, all \
        the .pkl files will be concatenated into one dataset.
    labels: dict
        Dictionary collected by the query_wikidata_dump function when collect_labels is set to True.
    return_: bool
        Boolean indicating if the built dataset should be returned on top of being written on disk.
    dump_date: str
        String indicating the date of the Wikidata dump used. It is used in the readme of the dataset.

    Returns
    -------
    edges: pandas.DataFrame
        DataFrame containing the edges between entities of the graph.
    attributes: pandas.DataFrame
        DataFrame containing edges linking entities to their attributes.
    entities: pandas.DataFrame
        DataFrame containing a list of all entities & attributes with their Wikidata IDs and labels.
    relations: pandas.DataFrame
        DataFrame containing a list of all relations with their Wikidata IDs and labels.
    """

    if path[-1] != '/':
        path = path+'/'
    path_pickle = path + 'pickles/'
    n_files = len([name for name in os.listdir(path_pickle) if name[-4:] == '.pkl'])
    df = concatpkls(n_files, path_pickle)

    ents = list(df['headEntity'].unique())
    feats = list(set(df['tailEntity'].unique()) - set(ents))
    ent2ix = {ent: i for i, ent in enumerate(ents + feats)}
    ix2ent = {i: ent for ent, i in ent2ix.items()}

    tmp = df['relation'].unique()
    rel2ix = {rel: i for i, rel in enumerate(tmp)}
    ix2rel = {i: rel for rel, i in rel2ix.items()}

    df['headEntity'] = df['headEntity'].apply(lambda x: ent2ix[x])
    df['tailEntity'] = df['tailEntity'].apply(lambda x: ent2ix[x])
    df['relation'] = df['relation'].apply(lambda x: rel2ix[x])

    nodes = pd.DataFrame([[i, ix2ent[i]] for i in range(len(ents))],
                         columns=['entityID', 'wikidataID'])
    entities = pd.DataFrame([[i, ix2ent[i]] for i in range(len(ix2ent))],
                            columns=['entityID', 'wikidataID'])
    relations = pd.DataFrame([[i, ix2rel[i]] for i in range(len(ix2rel))],
                             columns=['relationID', 'wikidataID'])

    if multi_lingual is not None:
        assert isinstance(multi_lingual, list)
        for lang in multi_lingual:
            nodes[lang+'_label'] = nodes['wikidataID'].apply(multi_lingual_relabel, args=(labels,lang))
            entities[lang+'_label'] = entities['wikidataID'].apply(multi_lingual_relabel, args=(labels,lang))
            relations[lang+'_label'] = relations['wikidataID'].apply(multi_lingual_relabel, args=(labels,lang))
    else:
        nodes['label'] = nodes['wikidataID'].apply(relabel, args=(labels,))
        entities['label'] = entities['wikidataID'].apply(relabel, args=(labels,))
        relations['label'] = relations['wikidataID'].apply(relabel, args=(labels,))   

    edges_mask = df.tailEntity.isin(df['headEntity'].unique())
    edges = df.loc[edges_mask, ['headEntity', 'tailEntity', 'relation']]
    attributes = df.loc[~edges_mask, ['headEntity', 'tailEntity', 'relation']]

    write_csv(edges, path + 'edges.tsv')
    write_csv(attributes, path + 'attributes.tsv')
    write_ent_dict(nodes, path + 'nodes.tsv')
    write_ent_dict(entities, path + 'entities.tsv')
    write_rel_dict(relations, path + 'relations.tsv')
    write_readme(path+'readme.md',
                 n_core_ents=attributes['headEntity'].nunique(),
                 n_attrib_ents=attributes['tailEntity'].nunique(),
                 n_core_rels=edges['relation'].nunique(),
                 n_attrib_rels=attributes['relation'].nunique(),
                 n_core_facts=len(edges),
                 n_attrib_facts=len(attributes),
                 dump_date=dump_date)

    if return_:
        return edges, attributes, entities, relations

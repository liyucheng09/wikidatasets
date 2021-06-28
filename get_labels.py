from wikidatasets.processFunctions import query_wikidata_dump, build_dataset, query_wikidata_dump_with_multi_processing
n_lines = 81933324
path = 'parsed_5/'
dump_path = '/cfs/cfs-daenbe5b/latest-all.json.bz2'

query_wikidata_dump_with_multi_processing(dump_path, path, n_lines, collect_labels=True, multi_lingual=True, skip_bytes=60000000000, num_procs=16, memory_lines=6000000)

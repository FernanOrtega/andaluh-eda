# -*- coding: utf-8 -*-

from andaluh import epa
import time
import codecs
from multiprocessing import Pool

ENTRIES_FILE = 'data/entries_es.txt'
MAPPING_CSV = 'data/entries_es_and2.csv'
# ENTRIES_FILE = 'tests/entries_out_mini.txt'
# MAPPING_CSV = 'tests/entries_mapping_mini.csv'


if __name__ == '__main__':
    vafs = [u'ç', u's', u'z', u'h']
    vvfs = [u'j', u'h']

    t_init = time.time()
    pool = Pool()
    with codecs.open(ENTRIES_FILE, encoding='utf-8', mode='r') as f_entries:
        content = f_entries.read().replace("\r\n\r\n", "\r\n")
        and_variants_results_pool = [pool.apply_async(epa, args=(content, vaf, vvf)) for vaf in vafs for vvf in vvfs]
        and_variants_results = [result.get() for result in and_variants_results_pool]

        matrix_result = [', '.join(row) for row in zip(*[content.split("\r\n")] + [and_variant_result.split("\r\n")
                                                                                   for and_variant_result in
                                                                                   and_variants_results])]
        with codecs.open(MAPPING_CSV, encoding='utf-8', mode='w') as f_mapping:
            f_mapping.write(u'cas, and_çj, and_sj, and_zj, and_hj, and_çh, and_sh, and_zh, and_hh\n')
            for row_str in matrix_result:
                f_mapping.write("%s\n" % row_str)
    t_total1 = time.time() - t_init
    print('Time: {}'.format(t_total1))

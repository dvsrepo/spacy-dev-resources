#!/usr/bin/env python

from __future__ import unicode_literals, print_function

import plac
import multiprocessing
import joblib
import time
from os import path
import os
import bz2
import ujson
from preshed.counter import PreshCounter
from joblib import Parallel, delayed
import io


import spacy
from spacy.en import English
from spacy.strings import StringStore
from spacy.attrs import ORTH
from spacy.tokenizer import Tokenizer
from spacy.vocab import Vocab

#import madoka

DEFAULT_CORES = multiprocessing.cpu_count()

def iter_comments(loc):
    with io.open(loc, 'r') as file_:
        for line in file_:
            yield line

def parallelize(func, iterator, n_jobs, lang_class):
    Parallel(n_jobs=n_jobs)(delayed(func)(*item, lang_class) for item in iterator)

def merge_counts(locs, out_loc):
    string_map = StringStore()
    counts = PreshCounter()
    counts_docs = PreshCounter()
    for loc in locs:
        with io.open(loc, 'r', encoding='utf8') as file_:
            for line in file_:
                freq, word = line.strip().split('\t', 1)
                orth = string_map[word]
                counts.inc(orth, int(freq))
                counts_docs.inc(orth, 1)
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for orth, count in counts:
            string = string_map[orth]
            file_.write('%d\t%d\t%s\n' % (count, counts_docs[orth], string))

def count_freqs(input_loc, output_loc, LangClass):
    start = time.time()
    print('INFO: Processing ', input_loc)
    vocab = LangClass.Defaults.create_vocab()
    tokenizer = LangClass.Defaults.create_tokenizer()
    #Tokenizer(vocab,path.join(LangClass.default_data_dir(), 'tokenizer'))

    counts = PreshCounter()
    for text in iter_comments(input_loc):
        doc = tokenizer(text)
        doc.count_by(ORTH, counts=counts)

    with io.open(output_loc, 'w', encoding='utf8') as file_:
        for orth, freq in counts:
            string = tokenizer.vocab.strings[orth]
            if not string.isspace():
                file_.write('%d\t%s\n' % (freq, string))
    end = time.time()-start
    print('INFO: File {} took {} min '.format(input_loc, end/60))

@plac.annotations(
    input_loc=("Location of input file list"),
    freqs_dir=("Directory for frequency files"),
    output_loc=("Location for output file"),
    lang_name=("Language"),
    n_jobs=("Number of workers", "option", "n", int),
    skip_existing=("Skip inputs where an output file exists", "flag", "s", bool),
)
def main(input_loc, freqs_dir, output_loc, lang_name='en', n_jobs=DEFAULT_CORES, skip_existing=False):
    tasks = []
    outputs = []

    LangClass = spacy.util.get_lang_class(lang_name)

    for input_path in open(input_loc):
        input_path = input_path.strip()
        if not input_path:
            continue
        filename = input_path.split('/')[-1]
        output_path = path.join(freqs_dir, filename.replace('bz2', 'freq'))
        outputs.append(output_path)
        if not path.exists(output_path) or not skip_existing:
            tasks.append((input_path, output_path))

    if tasks:
        parallelize(count_freqs, tasks, n_jobs, LangClass)

    print("INFO: Merging the counts to ", output_loc)
    merge_counts(outputs, output_loc)


if __name__ == '__main__':
    plac.call(main)

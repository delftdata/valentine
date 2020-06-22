from algorithms.embdi.EmbDI.embeddings_quality import embeddings_quality
from algorithms.embdi.EmbDI.entity_resolution import entity_resolution
from algorithms.embdi.EmbDI.schema_matching import schema_matching, match_columns
from algorithms.embdi.EmbDI.logging import *
from algorithms.embdi.EmbDI.utils import remove_prefixes
import os

def test_driver(embeddings_file, df, configuration=None):
    test_type = configuration['experiment_type']
    info_file = configuration['dataset_info']
    if test_type == 'EQ':
        if configuration['training_algorithm'] == 'fasttext':
            newf = embeddings_file
            mem_results.res_dict = embeddings_quality(newf, configuration)
        else:
            newf = remove_prefixes(configuration['flatten'], embeddings_file)
            mem_results.res_dict = embeddings_quality(newf, configuration)
            os.remove(newf)
    elif test_type == 'ER':
        mem_results.res_dict = entity_resolution(embeddings_file, configuration, df=df, info_file=info_file)
    elif test_type == 'SM':
        mem_results.res_dict = schema_matching(df, embeddings_file, configuration)
    else:
        raise ValueError('Unknown test type.')


def match_driver(embeddings_file, df, configuration):
    test_type = configuration['experiment_type']
    info_file = configuration['dataset_info']
    print('Extracting matched tuples')
    m_tuples = entity_resolution(embeddings_file, configuration, df=df, info_file=info_file, 
                                task='match')
    # print('Extracting matched columns')
    # m_columns = match_columns(df, embeddings_file)

    return m_tuples, []


import pandas as pd
from multiprocessing import Pool, cpu_count

def get_inv_dict(chunk):
    inv_dict = {}
    for index, row in chunk.iterrows():
        for and_value in row[1:]:
            if and_value not in inv_dict:
                inv_dict[and_value] = {row[0]}
            else:
                inv_dict[and_value].add(row[0])

    return inv_dict


def merge_inv_dict(dict1, dict2):
    return {k: {i for j in (dict1.get(k, {}), dict2.get(k, {})) for i in j} for k in set(dict1) | set(dict2)}


def get_inv_df_counts(df):
    chunk_size = int(df.shape[0] / cpu_count())
    chunks = [df.iloc[i:i + chunk_size, :] for i in range(0, df.shape[0], chunk_size)]

    pool = Pool()
    result = pool.map(get_inv_dict, chunks)

    inv_dict = result[0]
    for inv_dict_i in result[1:]:
        inv_dict = merge_inv_dict(inv_dict, inv_dict_i)

    inv_dict_counts = [[key, value, len(value)] for key, value in inv_dict.items()]

    return pd.DataFrame(inv_dict_counts, columns=["and", "cas", "cas_count"])
from __future__ import print_function
import sys
import csv
import pandas as pd
import numpy as np
import os
import time
from os import listdir
import ntpath
from collections import Set
from collections import defaultdict
import re
# import operator
# from sqlalchemy import create_engine
import configparser
from itertools import chain, combinations


LOWER = 'LL'  # [a-z]
UPPER = 'LU'  # [A-Z]
DIGIT = 'D'  # [0-9]
OTHERS = 'S'  # All other symbols


def read_DB_table(table_name, config_file):  # see values in a table
    conf = configparser.ConfigParser()
    conf.read(config_file)

    jdbc = conf['default']['db_type'].strip();
    db = conf['default']['dbase'].strip();
    user = conf['default']['user'].strip();
    host = conf['default']['host'].strip();
    port = conf['default']['port'].strip();
    engine_str = jdbc + '://' + user + '@' + host + ':' + port + '/' + db
    if "postgres" in jdbc:
        sql_str = "SELECT * FROM " + str(table_name)
        try:
            df = pd.read_sql_query(sql_str, con=engine_str)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[1])
            return None

        


def get_number_of_tuples_in_DB_table(table_name, config_file):
    conf = configparser.ConfigParser()
    conf.read(config_file)

    jdbc = conf['default']['db_type'].strip();
    db = conf['default']['dbase'].strip();
    user = conf['default']['user'].strip();
    host = conf['default']['host'].strip();
    port = conf['default']['port'].strip();
    engine_str = jdbc + '://' + user + '@' + host + ':' + port + '/' + db
    if "postgres" in jdbc:
        sql_str = "SELECT COUNT(*) FROM " + str(table_name)
        try:
            c = pd.read_sql_query(sql_str, con=engine_str)
            return c
        except:
            print("Unexpected error:", sys.exc_info()[1])
            return 0
    
    
def get_csv_fnames_list(dir_name):
    csv_tables_names = []
    data_path = os.path.abspath(dir_name);
    if csv_tables_names:
        for i in range(len(csv_tables_names)):
            csv_datafreames.remove(csv_tables_names[0])     
    file_extension = '.csv'
    try:
        filenames = listdir(data_path)
    except Exception as e:
        if hasattr(e, 'message'):
            print ("Error occured (", a, ")")
        else:
            print ("Error occured (", e, ")")        
        return None
    return [filename for filename in filenames if filename.endswith( file_extension ) ]



def read_table(tab_name):
    t_name = ntpath.basename(tab_name)
    try:
        df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                         quoting=csv.QUOTE_ALL, doublequote=True)
    except ValueError:
        try:
            df = pd.read_csv(filepath_or_buffer=tab_name, dtype=object, delimiter=',', low_memory=False,
                             quoting=csv.QUOTE_ALL, doublequote=True, encoding="ISO-8859-1")
        except:
            print("Error reading csv file .. file encoding is not recognizable")
            return None
    return df
    # return CA


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def get_fname(path):
    fname = path_leaf(path)
    fname_no_ext = fname.rsplit('.', 1)
    return fname_no_ext[0]


def check_double_quote(a):
    b = a
    if ',' in str(a):
        b = "\"" + str(a) + "\""
    return b


def all_subsets(ss):
    return chain(*map(lambda x: combinations(ss, x), range(0, len(ss) + 1)))


def sub_lists_of_size_L(lst, atts):
    subLists = []
    for i in range(len(subLists)):
        subLists.remove(subLists[0])
    aLists = all_subsets(lst)
    for a in aLists:
        if len(a) == 0:
            continue
        if len(a) == atts:
            subLists.append(list(a))
    return subLists


def index_table_attributes(df):
    index = []
    # idx = index_attribute(df[df.columns[0]])

    for i in range(len(df.columns)):
        idx = index_attribute(df[df.columns[i]])
        index.append(idx)
    return index


# return idx

def index_attribute(A):
    di = dict()
    # di = dict([i, 0] for i in A)
    for i in A:
        if i in di:
            di[i] += 1
        else:
            di[i] = 1
    return di




def get_att_details(att):
    avg_len = 0
    max_len = 0
    num_int = 0
    num_float = 0
    num_text = 0
    thresh = 0.99
    details = dict()
    dtype_dict = dict()
    dtype_dict["typeInt"] = dict()
    dtype_dict["typeFloat"] = dict()
    dtype_dict["typeText"] = dict()
    dtype = ""
    no_non_empty = 0
    att_no_null_values = att.dropna()
    idxA = att_no_null_values.index.tolist()
    no_non_empty = len(att_no_null_values)
    min_len = len(att_no_null_values[idxA[0]])
    for ix in idxA:
        # find min, max, average length of the values
        v = att_no_null_values[ix]
        lv = len(str(v))
        avg_len += lv
        max_len = max(max_len, lv)
        min_len = min(min_len, lv)

        # check the types of the values
        try:
            val = int(v)
            num_int += 1
            if lv in dtype_dict["typeInt"]:
                dtype_dict["typeInt"][lv] += 1
            else:
                dtype_dict["typeInt"][lv] = 1
        except ValueError:
            try:
                val = float(v)
                num_float += 1
                if lv in dtype_dict["typeFloat"]:
                    dtype_dict["typeFloat"][lv] += 1
                else:
                    dtype_dict["typeFloat"][lv] = 1
            except ValueError:
                num_text += 1
                if lv in dtype_dict["typeText"]:
                    dtype_dict["typeText"][lv] += 1
                else:
                    dtype_dict["typeText"][lv] = 1
    if no_non_empty > 0:
        avg_len /= no_non_empty
    details["max_len"] = max_len
    details["avg_len"] = avg_len
    details["min_len"] = min_len
    if num_int / no_non_empty > thresh:
        lens = sorted(dtype_dict["typeInt"].items(), key=lambda kv: kv[1], reverse=True)
        aa, bb = lens[0]
        if (bb / num_int > thresh) and (min_len > 2):
            dtype = "code"
        else:
            dtype = "integer"
    if num_float / no_non_empty > thresh:
        dtype = "float"
    if dtype == "":
        lens = sorted(dtype_dict["typeText"].items(), key=lambda kv: kv[1], reverse=True)
        if len(lens) > 0:
            aa, bb = lens[0]
            if bb / num_text > thresh:
                dtype = "code"
            else:
                dtype = "text"
            details["lens"] = lens
        else:
            details["lens"] = []
    details["dtype"] = dtype
    
    return details


def get_df_details(df):
    df_details = dict()
    df_details.clear()
    for att_id in range(len(df.columns)):
        att = df[df.columns[att_id]]
        details = get_att_details(att)
        df_details[att_id] = details
        df_details[att_id]["att_name"] = df.columns[att_id]
    return df_details


def create_gms_dict_att(B, att_id, df_details):
    l = len(B)
    di = dict()
    di.clear()
    req_pos = []
    min_coverage = 0.2
    gms_min_recs = 6

    for u in range(len(req_pos)):
        req_pos.remove(req_pos[0])
    if one_value_att(B):
        return di, req_pos
    if (df_details[att_id]["dtype"] == "integer") or (df_details[att_id]["dtype"] == "float"):
        return di, req_pos
    if df_details[att_id]["tg_vs_ng"] == "tokens":
        gms = create_tokgram_att(B)
    else:
        gms = create_ngram_att(B, df_details[att_id]["avg_len"])
    pl = gm_pos_info(gms, gms_min_recs)
    pos = sorted(pl.items(), key=lambda kv: kv[1], reverse=True)

    for el in pos:
        a, b = el
        if b / l > min_coverage:
            req_pos.append(a)
    if len(req_pos) > 0:
        for gm in gms:
            a = gm
            a11 = a.rsplit('::', 1)
            if a11[1] in req_pos:
                di[gm] = gms[gm]
    return di, req_pos


def create_gms_dict_tab(df, df_details):
    tab_gms = dict()
    tab_gms.clear()
    cand_list_atts = []
    for uu in range(len(cand_list_atts)):
        cand_list_atts.remove(cand_list_atts[0])
    for att_id in range(len(df.columns)):
        B = df[df.columns[att_id]]
        di, req_pos = create_gms_dict_att(B, att_id, df_details)
        if len(di) > 0:
            tab_gms[att_id] = di
            cand_list_atts.append(att_id)
            df_details[att_id]["gms_pos"] = req_pos

    return tab_gms, cand_list_atts, df_details


def create_rev_gms_dict_tab(df, df_details, cand_cols_list):
    tab_rgms = dict()
    tab_rgms.clear()
    for str_att_id in cand_cols_list:
        att_id = int(str_att_id)
        A = df[df.columns[att_id]]
        AA = A.dropna()
        if df_details[att_id]["tg_vs_ng"] == "tokens":
            rgms = create_rev_tokgram_att(AA, df_details[att_id]["gms_pos"])
        else:
            rgms = create_rev_ngram_att(AA, df_details[att_id]["avg_len"], df_details[att_id]["gms_pos"])
        if len(rgms) > 0:
            tab_rgms[att_id] = rgms

    return tab_rgms


def create_ngram_att(AA, avg_len, req_list=[]):
    d = dict()
    A = AA.dropna()
    qgram = int(avg_len)
    if qgram > 30:  # strings are long -- no need for qgrams
        #         print(qgram, 'Exceeded ....')
        idxA = A.index.tolist()
        for i in idxA:
            V = str(A[i])
            L = ("{0:s}::{1:s}".format(V, '0'))
            if L in d:
                d[L].append(i)  # positions in strings start from 0
            else:
                d[L] = []
                d[L].append(i)
    else:
        idxA = A.index.tolist()
        for i in idxA:
            V = str(A[i])
            # if pd.isnull(V):
            #     continue
            kmin = min(3, len(V)) - 1
            k_range = list(range(len(V) - 1, kmin - 1, -1))

            for k in k_range:
                if len(req_list) == 0:
                    req_pos = range(len(V) - k)
                else:
                    req_pos = req_list
                for j in req_pos:
                    j_int = int(j)
                    L = V[j_int:j_int + k + 1]
                    # if pd.isnull(L):
                    #     break
                    L = ("{0:s}::{1:s}".format(L, str(j)))
                    if L in d:
                        a = d[L][len(d[L]) - 1]
                        if a == i:  # Another occurrence in the same record
                            d[L][len(d[L]) - 1] = a
                        else:
                            d[L].append(i)  # occurrence in a new record
                    else:
                        d[L] = []
                        d[L].append(i)
    return d


def create_tokgram_att(AA, req_list=[]):
    d = dict()
    A = AA.dropna()
    idxA = A.index.tolist()
    for i in idxA:
        V = str(A[i])
        # if pd.isnull(V):
        #     continue
        toks1 = re.split('(\d*\.\d+|\W)', V)
        toks = [t for t in toks1 if len(t) > 0]
        for k in range(len(toks) - 1, -1, -1):
            if len(req_list) == 0:
                req_pos = range(len(toks) - k)
            else:
                req_pos = req_list
            for j in req_pos:
                j_int = int(j)
                L = ""
                if toks[j_int].isspace():
                    continue
                if toks[j_int + k].isspace():
                    continue
                for jj in range(k + 1):
                    L += toks[j_int + jj]
                # if pd.isnull(L):
                #     break
                L = ("{0:s}::{1:s}".format(L, str(j)))
                if len(L) <= 4:
                    if len(V) > 1:
                        continue
                if L in d:
                    a = d[L][len(d[L]) - 1]
                    if a == i:  # Another occurrence in the same record
                        d[L][len(d[L]) - 1] = a
                    else:
                        d[L].append(i)  # occurrence in a new record
                else:
                    d[L] = []
                    d[L].append(i)
    return d


def create_rev_ngram_att(AA, avg_len, req_list=[]):
    d = dict()
    A = AA.dropna()
    qgram = int(avg_len)
    if qgram > 30:  # strings are long -- no need for qgrams
        #         print(qgram, 'Exceeded ....')
        idxA = A.index.tolist()
        for i in idxA:
            V = str(A[i])
            L = ("{0:s}::{1:s}".format(V, '0'))
            # if pd.isnull(V):
            #     continue
            d[i] = L
    else:
        idxA = A.index.tolist()
        for i in idxA:
            V = str(A[i])
            lenv = len(V)
            # if pd.isnull(V):
            #     continue
            kmin = min(3, len(V)) - 1
            k_range = list(range(len(V) - 1, kmin - 1, -1))
            gms_list = []
            for uu in range(len(gms_list)):
                gms_list.remove(gms_list[0])

            for k in k_range:
                if len(req_list) == 0:
                    req_pos = range(lenv - k)
                else:
                    req_pos = req_list
                for j in req_pos:
                    j_int = int(j)
                    if (j_int > lenv) or (j_int + k > lenv):
                        continue
                    L = V[j_int:j_int + k + 1]
                    # if pd.isnull(L):
                    #     break
                    L = ("{0:s}::{1:s}".format(L, str(j)))
                    if (L in gms_list):
                        continue
                    gms_list.append(L)
            d[i] = gms_list
    return d


def create_rev_tokgram_att(AA, req_list=[]):
    d = dict()
    A = AA.dropna()
    idxA = A.index.tolist()
    for i in idxA:
        V = str(A[i])
        # if pd.isnull(V):
        #     continue
        gms_list = []
        for uu in range(len(gms_list)):
            gms_list.remove(gms_list[0])
        toks1 = re.split('(\d*\.\d+|\W)', V)
        toks = [t for t in toks1 if len(t) > 0]
        len_tok = len(toks)
        for k in range(len_tok - 1, -1, -1):
            if len(req_list) == 0:
                req_pos = range(len_tok - k)
            else:
                req_pos = req_list
            for j in req_pos:
                j_int = int(j)
                if (j_int + k >= len_tok):
                    continue
                L = ""
                if toks[j_int].isspace():
                    continue
                if toks[j_int + k].isspace():
                    continue
                for jj in range(k + 1):
                    L += toks[j_int + jj]
                # if pd.isnull(L):
                #     break
                L = ("{0:s}::{1:s}".format(L, str(j)))
                if len(L) <= 4:
                    if len(V) > 1:
                        continue
                if (L in gms_list):
                    continue
                gms_list.append(L)
        d[i] = gms_list
    return d


# Just use the full value as an ngram
def create_special_grams(A):
    d = dict()
    for i in range(len(A)):
        V = str(A[i])
        if V in d:
            d[V].append((i, ['x']))  # positions in strings start from 1
        else:
            d[V] = []
            d[V].append((i, ['x']))
    return d


def create_special_grams_new(A):
    d = dict()
    for i in range(len(A)):
        V = str(A[i])
        if V in d:
            d[V].append(i)  # positions in strings start from 1
        else:
            d[V] = []
            d[V].append(i)
    return d



# Decide on using n-grams or tokens for all attributes in a dataframe
# the output is a list of two strings "tokens" or "ngrams"
def tokens_vs_ngrams(A):
    d = dict()
    AA = A.dropna()
    idxAA = AA.index.tolist()
    for i in idxAA:
        V = str(AA[i])
        toks1 = re.split('(\d*\.\d+|\W)', V)
        toks = [t for t in toks1 if len(t) > 0]
        L = len(toks)
        if L in d:
            d[L] += 1
        else:
            d[L] = 1
    sp = 0
    for L in d:
        sp += L * d[L]
    if sp > 2 * len(AA):
        return "tokens"
    else:
        return "ngrams"


# Decide on using n-grams or tokens for all attributes in a dataframe
# the output is a list of two strings "tokens" or "ngrams"
def tokens_vs_ngrams_df(df, df_details):
    for col_id in range(len(df.columns)):
        A = df[df.columns[col_id]]
        tg_vs_ng = tokens_vs_ngrams(A)
        df_details[col_id]["tg_vs_ng"] = tg_vs_ng

    return df_details








def check_majority_condition(sb_gms, tgt_att, majority_ratio):
    if len(sb_gms) / len(tgt_att) < majority_ratio:
        return False
    sb_set = set()
    sb_set.clear()
    for sb_gm in sb_gms:
        sb_set.add(sb_gm)
    tgt_set = set(tgt_att)
    intersect_set = sb_set & tgt_set
    if len(intersect_set) / len(tgt_set) > majority_ratio:
        if len(tgt_set) - len(intersect_set) < 20:
            return True
    return False


def get_best_pTP(sub_TPs):
    TP = None
    TP_max_len = 0
    for sub_TP in sub_TPs:
        ((a, b), c) = sub_TP
        if TP:
            if TP_max_len < len(b):
                TP_max_len = len(b)
                TP = sub_TP
        else:
            TP_max_len = len(b)
            TP = sub_TP
    if not TP:
        print(sub_TPs)
    return TP


def num_recs_contining_ng(subdf, ng):
    idxs = subdf.index.tolist()
    S = 0
    matching = 0
    for idx in idxs:
        str1 = re.sub(' +', ' ', str(ng))
        str2 = re.sub(' +', ' ', str(subdf[idx]))
        if str1 in str2:
            S += 1
        if str1 == str2:
            matching += 1
    if matching / len(subdf) < 0.9:
        return S, 'Partial'
    return S, 'Exact'



def find_special_rules(att1_ngs, att1, att2, max_distinct_dep_values = 3, majority_ratio = 0.99):
    TPs = []
    max_noise = 1
    min_recs = 6
    # dep_name = att2.columns[0]
    for i in range(len(TPs)):       # Clear idx if it contains elements
        TPs.remove(TPs[0])
    for ng in att1_ngs:
        idx = []
        sd_len = 0
        for i in range(len(idx)):     # Clear idx if it contains elements
            idx.remove(idx[0])
        for el in att1_ngs[ng]:
            (a,b) = el
            idx.append(a)
        subdf = pd.DataFrame(att2.loc[idx])
        Num, matching = num_recs_contining_ng(subdf[subdf.columns[0]], ng)
        # return subdf
        if (len(subdf) < 100):
            if (Num >= len(subdf) - max_noise):
                if matching == 'Exact':
                    ele = '\\A*' + ng + '\\A*'
                    TP = ((ele, ng), subdf[subdf.columns[0]].index.tolist())
                if matching == 'Partial':
                    ele = '\\A*' + ng + '\\A*'
                    TP = ((ng, ele), subdf[subdf.columns[0]].index.tolist())
                if not pd.isnull(ng):
                    TPs.append(TP)
        else:
            if (Num / len(subdf) > majority_ratio):
                if matching == 'Exact':
                    ele = '\\A*' + ng + '\\A*'
                    TP = ((ele, ng), subdf[subdf.columns[0]].index.tolist())
                if matching == 'Partial':
                    ele = '\\A*' + ng + '\\A*'
                    TP = ((ng, ele), subdf[subdf.columns[0]].index.tolist())
                if not pd.isnull(ng):
                    TPs.append(TP)
    return TPs





def find_general_rule(att1, det_id, att2, dep_id, df_details):
    a_in_b = 0
    b_in_a = 0
    a_equals_b = 0
    start = 0
    middle = 0
    end = 0
    TPs = []
    idxs = att1.index.tolist()
    for i in range(len(TPs)):       # Clear idx if it contains elements
        TPs.remove(TPs[0])
    for idx in idxs:
        v1 = str(att1[idx])
        v2 = str(att2[idx])
        if (len(str(v1)) > len(str(v2))):
            if (v1.startswith(v2)):
                b_in_a += 1
                start += 1
            elif (v1.endswith(v2)):
                b_in_a += 1
                end += 1
            else:
                if(v2 in v1):
                    middle += 1 
                    b_in_a += 1
        else:
            if (v2.startswith(v1)):
                a_in_b += 1
                start += 1
            elif (v2.endswith(v1)):
                a_in_b += 1
                end += 1
            else:
                if(v1 in v2):
                    middle += 1 
                    a_in_b += 1
    lhs = ""
    rhs = ""
    if a_in_b < b_in_a:
        if max(start, middle, end) == start:
            lhs = df_details[det_id]["att_name"] + ":(" + df_details[dep_id]["att_name"] + '\\A*)' 
            rhs = df_details[dep_id]["att_name"]
        elif max(start, middle, end) == middle:
            lhs = df_details[det_id]["att_name"] + ":(" + '\\A*' + df_details[dep_id]["att_name"] + '\\A*)' 
            rhs = df_details[dep_id]["att_name"]
        elif max(start, middle, end) == end:
            lhs = df_details[det_id]["att_name"] + ":(" + '\\A*' + df_details[dep_id]["att_name"] + ")"
            rhs = df_details[dep_id]["att_name"]
    else:
        if max(start, middle, end) == start:
            lhs = df_details[dep_id]["att_name"] + ":(" + df_details[det_id]["att_name"] + '\\A*)' 
            rhs = df_details[det_id]["att_name"]
        elif max(start, middle, end) == middle:
            lhs = df_details[dep_id]["att_name"] + ":(" + '\\A*' + df_details[det_id]["att_name"] + '\\A*)' 
            rhs = df_details[det_id]["att_name"]
        elif max(start, middle, end) == end:
            lhs = df_details[dep_id]["att_name"] + ":(" + '\\A*' + df_details[det_id]["att_name"] + ")"
            rhs = df_details[det_id]["att_name"]
    TP = ((lhs, rhs), att1.index.tolist())
    TPs.append(TP)        
    return TPs





def find_substring_dependency(A, det_id, B, dep_id, df_details):
    if not check_substring_dependency(A, B):
        return [], []
    TPs = []
    vio_recs = []
    df_A = pd.DataFrame(A)
    df_B = pd.DataFrame(B)
    TPs = find_general_rule(A, det_id, B, dep_id, df_details)  #check
    vrec_idx = get_sp_vio_records(A, B)
    return TPs, vrec_idx






def equal(L1, L2):
    s1 = set(L1.index.tolist())
    s2 = set(L2.index.tolist())
    if s1 == s2:
        return True
    else:
        return False


def intersect(L1, L2):
    return (L1 & L2)


def check_sub_strings(a1, a2, num_atts):
    a11 = a1.split('||')
    a22 = a2.split('||')
    i = 0
    for ii in range(num_atts):
        if (a11[ii] in a22[ii]):
            i += 1
    #     print ("i = ", i)
    if i == num_atts:
        return True
    else:
        return False


def find_max_len_gs(TPNs, allowed_noise):
    TPMs = []
    majority_ratio = (100.0 - float(allowed_noise)) / 100.0
    for ii in range(len(TPMs)):  # Clear TPMs if it contains elements
        TPMs.remove(TPMs[0])
    if (len(TPNs)) < 2:
        return TPNs
    (a, b), c = TPNs[0]
    a1 = a.split('::', 1)
    a2 = a1[0].split('||')
    num_atts = len(a2)
    for TP in TPNs:
        found = False
        ((a1, dep1), b1) = TP
        a11 = a1.rsplit('::', 1)
        for j in range(len(TPMs)):
            ((a2, dep2), b2) = TPMs[j]
            a22 = a2.rsplit('::', 1)
            if check_sub_strings(a11[0], a22[0], num_atts):
                if len(b2) / len(b1) >= majority_ratio:
                    found = True
        if not found:
            TPMs.append(TP)
    TPs = []
    for ii in range(len(TPs)):  # Clear TPRs if it contains elements
        TPs.remove(TPs[0])
    for tp in TPMs:
        found = False
        ((a1, dep1), b1) = tp
        a11 = a1.rsplit('::', 1)
        for j in range(len(TPs)):
            ((a2, dep2), b2) = TPs[j]
            a22 = a2.rsplit('::', 1)
            if check_sub_strings(a11[0], a22[0], num_atts):
                # if equal(b1, b2):
                if len(b1) / len(b2) >= majority_ratio:
                    #                     TPs[j] = tp
                    found = True
        if not found:
            TPs.append(tp)
    return TPs


def tok_pos_info_det(TPs):
    pos_lists = []
    for ii in range(len(pos_lists)):
        pos_lists.remove(pos_lists[0])
    ((a, dep1), b) = TPs[0]
    a11 = a.rsplit('::', 1)
    a22 = a11[1].split(',')
    num_atts = len(a22)
    for i in range(num_atts):
        d = dict()
        d.clear()
        pos_lists.append(d)
    for TP in TPs:
        ((a, dep1), b) = TP
        a11 = a.rsplit('::', 1)
        a22 = a11[1].split(',')
        for jj in range(num_atts):
            tok_pos = a22[jj]
            if tok_pos in pos_lists[jj]:
                pos_lists[jj][tok_pos] += 1
            else:
                pos_lists[jj][tok_pos] = 1
    return pos_lists, num_atts


def get_dom_pos_det(TPs):
    pos_lists, num_atts = tok_pos_info_det(TPs)
    max_num_pos = 3
    max_distinct_pos_values = 10
    majority_ratio = 0.75
    ss = ""
    for ii in range(num_atts - 1):
        pos = sorted(pos_lists[ii].items(), key=lambda kv: kv[1], reverse=True)
        aa, bb = pos[0]
        ss += str(aa) + ','
    pos = sorted(pos_lists[num_atts - 1].items(), key=lambda kv: kv[1], reverse=True)
    aa, bb = pos[0]
    ss += str(aa)
    d_pos = [ss]
    return d_pos





def tok_pos_info_dep(TPs):
    pos_list = dict()
    for TP in TPs:
        ((a, dep1), b) = TP
        a11 = dep1.rsplit('::', 1)
        if len(a11) < 2:
            print(TP)
            continue
        tok_pos = a11[1]
        if tok_pos in pos_list:
            pos_list[tok_pos] += 1
        else:
            pos_list[tok_pos] = 1
    return pos_list


def get_dom_pos_dep(TPs):
    pos_list = tok_pos_info_dep(TPs)
    max_num_pos = 3
    max_distinct_pos_values = 10
    majority_ratio = 0.75
    pos = sorted(pos_list.items(), key=lambda kv: kv[1], reverse=True)
    ss = 0
    for e in pos_list:
        ss += pos_list[e]
    s_d = 0
    d_pos = []
    for ii in range(len(d_pos)):
        d_pos.remove(d_pos[0])
    if pos:
        ee = pos[0]
        d_pos.append(ee[0])
        return d_pos
    else:
        return None


def gm_pos_info(gms, gms_min_recs):
    pos_list = dict()
    pos_list.clear()
    for gm in gms:
        a = gm
        b = gms[gm]
        a11 = a.rsplit('::', 1)
        gm_pos = a11[1]
        if (gm_pos == 1):
            print("GM : ", gm, "   occured in : ", gms[gm], "  with size = ", len(b))
        if len(b) > gms_min_recs:
            if gm_pos in pos_list:
                for ele in b:
                    pos_list[gm_pos].add(ele)
            else:
                s = set()
                s.clear()
                for ele in b:
                    s.add(ele)
                pos_list[gm_pos] = s
    pos_count = dict()
    pos_count.clear()
    for pl in pos_list:
        pos_count[pl] = len(pos_list[pl])
    return pos_count


def find_det_candidates_level_L(clist, dep, lvl):
    det = [x for x in clist if x not in dep]
    dets_list_of_lists = sub_lists_of_size_L(det, lvl)
    return dets_list_of_lists


def prune_TPs_by_pos_info(TPs):
    if not TPs:
        return []
    d_pos_det = get_dom_pos_det(TPs)
    if d_pos_det:
        d_s_det = set()
        d_s_det.clear()
        d_s_det = set(d_pos_det)
        new_TPs = []
        for i in range(len(new_TPs)):
            new_TPs.remove(new_TPs[0])
        for TP in TPs:
            ((a, dep1), b) = TP
            tok = a.rsplit('::', 1)
            if tok[1] in d_s_det:
                new_TPs.append(TP)
        TPs = new_TPs
        if TPs:
            d_pos_dep = get_dom_pos_dep(TPs)
            if d_pos_dep:
                d_s_dep = set()
                d_s_dep.clear()
                d_s_dep = set(d_pos_dep)
                new_TPs = []
                for i in range(len(new_TPs)):
                    new_TPs.remove(new_TPs[0])
                for TP in TPs:
                    ((a, dep1), b) = TP
                    tok = dep1.rsplit('::', 1)
                    if tok[1] in d_s_dep:
                        new_TPs.append(TP)
        return new_TPs
    return []


def one_value_att(B):
    di = index_attribute(B)
    if len(di) == 1:
        return True
    if len(di) == 2:
        for e in di:
            if pd.isnull(e):
                return True
    return False


def get_records_by_token(tok, TPs, tab_df):
    for TP in TPs:
        ((a, dep), b) = TP
        if tok in a:
            idx = b.index.tolist()
            needed_df = pd.DataFrame(tab_df.loc[idx])
            return needed_df
    return []


def get_TPs_by_rec_idx(ix, TPs):
    n_TPs = []
    for i in range(len(n_TPs)):
        n_TPs.remove(n_TPs[0])
    for TP in TPs:
        ((a, dep), b) = TP
        
        if ix in b:
            n_TPs.append(TP)
    return n_TPs


def get_violating_records(TPs, dep_att):
    violating_recs = set()
    violating_recs.clear()
    for TP in TPs:
        ((a, dep), b) = TP
        idx = b
        violating_idxs = []
        for kk in range(len(violating_idxs)):
            violating_idxs.remove(violating_idxs[0])
        for dx in idx:
            s1 = str(dep_att[dx]).lower()
            s2 = dep.rsplit('::', 1)
            s3 = s2[0].lower()
            if not s3 in s1:
                violating_recs.add(dx)
    return violating_recs


def check_substring_dependency(A, B):
    num_checked_recs = 0
    sat_recs = 0
    sample_size = min(1000, len(A))
    AA = A.sample(sample_size)
    idx = AA.index.tolist()
    BB = B[idx]
    for i in idx:
        if pd.isnull(AA[i]) or pd.isnull(BB[i]):
            continue
        num_checked_recs += 1
        if (str(AA[i]) in str(BB[i])) or (str(BB[i]) in str(AA[i])):
            if min(len(str(AA[i])), len(str(BB[i]))) > 3:
                sat_recs += 1
        if num_checked_recs == 100:
            break
    if num_checked_recs == 0:
        return False
    if sat_recs / num_checked_recs > 0.95:
        return True
    return False


def sub_values_att(A, B):
    for i in range(len(A)):
        if pd.isnull(A[i]) or pd.isnull(B[i]):
            continue
        if (A[i] in B[i]):
            return A
        if (B[i] in A[i]):
            return B





def get_sp_vio_records(A, B):
    C = sub_values_att(A, B)
    vios = []
    for i in range(len(C)):
        if (re.sub(' +', ' ', str(C[i])) in re.sub(' +', ' ', str(A[i]))):
            if (re.sub(' +', ' ', str(C[i])) in re.sub(' +', ' ', str(B[i]))):
                continue
        vios.append(i)
    return vios


def get_coverage(TPs, len_df):
    re_cov = set()
    re_cov.clear()
    for T in TPs:
        (a, b) = T
        for d in b:
            re_cov.add(d)
    rec_ratio = len(re_cov) / len_df
    return rec_ratio


def create_gms_index(input_gms):
    sub_gms = dict()
    sub_gms.clear()
    for rec in input_gms.keys():
        rec_gms = input_gms[rec]
        if isinstance(rec_gms, str):
            sub_gms[gm] = []
            sub_gms[gm].append(rec)
        else:
            for gm in rec_gms:
                if gm in sub_gms:
                    sub_gms[gm].append(rec)
                else:
                    sub_gms[gm] = []
                    sub_gms[gm].append(rec)
    return sub_gms


def pfd_discovery(dets_gms, dep_gms, dep_rev_gms, dep_att, param_config):
    # find the Tableaux
    init_TPs = recursive_pfd(dets_gms, dep_gms, dep_rev_gms, param_config)
    pos_TPs = prune_TPs_by_pos_info(init_TPs)
    long_TPs = find_max_len_gs(pos_TPs, param_config["allowed_noise_delta"])
    final_TPs = ignore_TPs_When_RHS_always_the_same(long_TPs)
    vrec_idx = []
    if len(final_TPs) > 0:
        vrec_idx = get_violating_records(final_TPs, dep_att)
    return final_TPs, vrec_idx


def recursive_pfd(dets_gms, dep_gms, dep_rev_gms, param_config):
    TPs = []
    if len(dets_gms) == 1:
        s_TPs = find_rules(dets_gms[0], dep_gms[0], dep_rev_gms[0], param_config)
        return s_TPs


def find_rules(det_gms, dep_gms, dep_rev_gms, param_config):
    TPs = []
    check = 0
    max_noise = float(param_config["allowed_noise_delta"])
    ngs_min_recs = float(param_config["confidence_K"])
    majority_ratio = (100.0 - float(max_noise)) / 100.0
    allowed_null_ratio = 0.85
    
    for i in range(len(TPs)):  # Clear idx if it contains elements
        TPs.remove(TPs[0])
    for ng in det_gms.keys():
        sd_len = 0
        det_len = len(det_gms[ng])
        dep_list = det_gms[ng]
        if det_len < ngs_min_recs:
            continue
        dep_df_gms = dict()
        dep_df_gms.clear()
        null_values = 0
        for el in det_gms[ng]:
            if not (el in dep_rev_gms):
                null_values += 1
                continue
            dep_df_gms[el] = dep_rev_gms[el]
        if (null_values / det_len > allowed_null_ratio):
            continue
            
        sub_gms = create_gms_index(dep_df_gms)
        sub_TPs = []
        for i in range(len(sub_TPs)):  # Clear idx if it contains elements
            sub_TPs.remove(sub_TPs[0])
        for sub_gm in sub_gms:
            len_subgm = len(sub_gms[sub_gm])
            if (det_len < 100):
                cur_max_noise = np.ceil((det_len /  100) * max_noise)
                if (len_subgm >= len(dep_list) - cur_max_noise):
                    TP = ((ng, sub_gm), dep_list)
                    sub_TPs.append(TP)
                    break
            else:
                if check_majority_condition(sub_gms[sub_gm], dep_list, majority_ratio):
                    TP = ((ng, sub_gm), dep_list)
                    sub_TPs.append(TP)
                    break
        if len(sub_TPs) > 0:
            best_TP = get_best_pTP(sub_TPs)
            if best_TP:
                TPs.append(best_TP)
    #         print(TPs)

    return TPs

def extract_part_of_dict(keys, mydict):
    ngs = dict()
    ngs.clear()
    for dx in range(len(keys)):
        ngs[dx] = mydict[keys[dx]]
    return ngs




def output_TPs_and_vios(output_args, TPs, vrec_idx, tgt_cover = 0.1):
    cover = get_coverage(TPs, len(output_args["data"]))
    DB = output_args["db"]

    if cover >= tgt_cover:
#             print("Writing the output ...")
        if DB:
            out_dir = os.path.join(output_args["dir_name"], output_args["tname"] + '/')
        else:
            out_dir = os.path.join(output_args["dir_name"], get_fname(output_args["tname"]) + '/')
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        det_str = ""
        dep_str = ""
        dets = output_args["dets"]
        dep = output_args["dep"]
        df = output_args["data"]
        det_cols = df.columns[dets]
        dep_cols = df.columns[dep]
        dep_att = df[df.columns[dep[0]]]


        for dt in det_cols:
            det_str += str(dt) + "__"
        for dp in dep_cols:
            dep_str += str(dp) + "__"
        tps_fname = out_dir + "Dep_" + det_str + '___' + str(dep_str) + '_PFDs.txt'
        f = open(tps_fname, "w")
        f.write("Coverage =  {0:s})\n".format(str(cover)))
        for TP in TPs:
            (a, b) = TP
            f.write(str(a) + "(Records affected = {0:s})\n".format(str(len(b))))
            f.write('======================================================\n')
        f.close()
        vios_fname = out_dir + "Dep_" + det_str + '___' + str(dep_str) + '_Vios.txt'
        #             # Find violating records
        #  Extract required information

        if len(vrec_idx) < 30 and len(vrec_idx) > 0:
            vrec_df = pd.DataFrame(df.loc[list(vrec_idx)])
            vrec_df.sort_index(inplace=True)
    #             print("Number of Violations = ", len(vrec_df))
            f = open(vios_fname, "w")
            idx = vrec_df.index.tolist()
            for ix in idx:
                a = vrec_df.loc[ix]
                det_str = ""
                for i in range(len(dets) - 1):
                    det_str += check_double_quote(str(a[dets][i])) + ","
                det_str += check_double_quote(str(a[dets][len(dets) - 1]))
                f.write(det_str + "  |=  " + check_double_quote(str(a[dep[0]])) + "\n")
            f.close()
        return True
#     print("No output to write...")
    return False


def ignore_TPs_When_RHS_always_the_same(TPs):
    if not TPs:
        return []
    dep_list = dict()
    nTPs = TPs
    if not nTPs:
        return []
    for TP in nTPs:
        ((a, dep1), b) = TP
        if dep1 in dep_list:
            dep_list[dep1] += 1
        else:
            dep_list[dep1] = 1
    if len(dep_list) > 1:
        return nTPs
    else:
        return []

def find_dependencies(param_config):
    data_dir = param_config["data_dir"]
    data_files = get_csv_fnames_list(data_dir)
    DB = 0
    results_dir = os.path.join(param_config["results_main_dir"], param_config["repo"])
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    dep_fname = os.path.join(results_dir, param_config["repo"] + "_Deps.txt")
    with open(dep_fname, "w") as fout:
        for f in data_files:
            start_time = time.time()
            tab_name = os.path.join(data_dir, f)
            df = read_table(tab_name)
            cols_len = len(df.columns)
            init_cols_list = range(cols_len)
            output_args = {
                    "tname" : tab_name,
                    "db" : DB,
                    "data" : df,
                    "dir_name" : results_dir
                }
            max_num_dets = 1
            
            df_details = get_df_details(df)
            df_details = tokens_vs_ngrams_df(df, df_details)
            gms, cand_cols_list, df_details = create_gms_dict_tab(df, df_details)
            rev_gms = create_rev_gms_dict_tab(df, df_details, cand_cols_list)
            for k in cand_cols_list:
                dep = [k]
                cols_list = cand_cols_list
                dfs = df
                dep_col = dfs.columns[dep[0]]
                B = dfs[dep_col]
                for lvl in range(max_num_dets):
                    dets_list_of_lists = find_det_candidates_level_L(cols_list, dep, lvl+1)
                    atts_to_be_removed = []
                    for att in range(len(atts_to_be_removed)):
                        atts_to_be_removed.remove(atts_to_be_removed[0]) # make sure that the list is empty
                    for dets in dets_list_of_lists:
                        TPs = []
                        vio_idx = []
                        if lvl == 0:
                            det_col = dfs.columns[dets[0]]
                            A = dfs[det_col]
                            TPs, vio_idx = find_substring_dependency(A, dets[0], B, dep[0], df_details)
                        if len(TPs) == 0:
                            dets_gms = extract_part_of_dict(dets, gms)
                            dets_rev_gms = extract_part_of_dict(dets, rev_gms)
                            dep_gms = extract_part_of_dict(dep, gms)
                            dep_rev_gms = extract_part_of_dict(dep, rev_gms)
                            TPs, vio_idx = pfd_discovery(dets_gms, dep_gms, dep_rev_gms, B, param_config)
                        output_args["dets"] = dets
                        output_args["dep"] = dep

                        valid_pfd = output_TPs_and_vios(output_args, TPs, vio_idx, param_config["min_acceptable_coverage"])
                        if valid_pfd:
                            generalized = check_for_generalization(TPs, gms, dets[0], dep, df)
                            print(tab_name+'::'+df.columns[dets]+'::'+df.columns[dep], '==>', generalized)
                            fout.write(f + "::"+ df.columns[dets[0]] + "<==>" + df.columns[dep[0]] + "\n")
                            for attrib in dets:
                                atts_to_be_removed.append(attrib)

                    new_cols_list = []
                    for ncl in range(len(new_cols_list)):
                        new_cols_list.remove(new_cols_list[0])
                    for att_id in cols_list:
                        if att_id in atts_to_be_removed:
                            continue
                        new_cols_list.append(att_id)
                    cols_list = new_cols_list
            end_time = time.time()
            spent_time = end_time - start_time
            print("Time spent to process table ({0:s}) is ({1:s})".format(f, str(spent_time)))
    fout.close()



def check_for_generalization(tps_l, grams, det, dep, df):
    req_gs = grams[det]
    if len(tps_l) == 1:
        return True
    ((a,b), c) = tps_l[0]
    pos = a.split('::')[1]
    dd = dict()
    dd.clear()
    A = df[df.columns[det]]
    B = df[df.columns[dep]]
    for gm in req_gs.keys():
        g = gm.split('::')[1]
        if g == pos:
            dd[gm] = req_gs[gm]
    full_value_gram = 0
    if int(pos) == 0:
        for d in dd.keys():
            d_idx = dd[d]
            d_v = d.split('::')[0]
            for idx in d_idx: 
                if d_v == A[idx]:
                    full_value_gram += 1
                    break;
            if full_value_gram > 5:
                break;
    if full_value_gram > 3:
        return False
    no_vios = 0
    for d_gs in dd.keys():
        if len(dd[d_gs]) > 1:
            try: 
                LHS = B[dd[d_gs]]
                if len(set(LHS)) > 1:
                    no_vios += 1
            except:
                continue
                # # print(e)
                # print('======================================')
                # print(dd[d_gs], len(B), '    det = ', det, '     dep = ', dep)
    if (no_vios / len(df)) > 0.005 or no_vios > 100:
        return False
#     new_tps = 
#     return new_tps, True
    return True





def find_pfds_csv(param_config):
    tab_name = param_config["tab_name"]
    df = read_table(tab_name)
    cols_len = len(df.columns)
    init_cols_list = range(cols_len)
    
    max_num_dets = 1
    
    df_details = get_df_details(df)
    df_details = tokens_vs_ngrams_df(df, df_details)
    gms, cand_cols_list, df_details = create_gms_dict_tab(df, df_details)
    results = dict()
    results.clear()
    new_gms = dict()
    final_gms = dict()
    final_gms.clear()
    for k in gms.keys():
        new_gms.clear()
        new_pats = {key:len(gms[k][key]) for key in gms[k].keys()}
        sorted_pats = sorted(new_pats.items(), key=lambda kv: kv[1], reverse=True)
        final_gms[k] = sorted_pats
    results ['patterns'] = final_gms
    results ['df_details'] = df_details
    results ['pfds'] = []
    for i in range(len(results['pfds'])):
        results['pfds'].remove(results['pfds'][0])
    rev_gms = create_rev_gms_dict_tab(df, df_details, cand_cols_list)
    for k in cand_cols_list:
        dep = [k]
        cols_list = cand_cols_list
        dfs = df
        dep_col = dfs.columns[dep[0]]
        B = dfs[dep_col]
        for lvl in range(max_num_dets):
            dets_list_of_lists = find_det_candidates_level_L(cols_list, dep, lvl+1)
            atts_to_be_removed = []
            for att in range(len(atts_to_be_removed)):
                atts_to_be_removed.remove(atts_to_be_removed[0]) # make sure that the list is empty
            for dets in dets_list_of_lists:
                TPs = []
                vio_idx = []
                if lvl == 0:
                    det_col = dfs.columns[dets[0]]
                    A = dfs[det_col]
                    TPs, vio_idx = find_substring_dependency(A, dets[0], B, dep[0], df_details)
                if len(TPs) == 0:
                    dets_gms = extract_part_of_dict(dets, gms)
                    dets_rev_gms = extract_part_of_dict(dets, rev_gms)
                    dep_gms = extract_part_of_dict(dep, gms)
                    dep_rev_gms = extract_part_of_dict(dep, rev_gms)
                    TPs, vio_idx = pfd_discovery(dets_gms, dep_gms, dep_rev_gms, B, param_config)
                cover = get_coverage(TPs, len(df))
                if cover > param_config["min_acceptable_coverage"]:
                    new_pfd = dict()
                    new_pfd.clear()
                    new_pfd['det'] = df.columns[dets]
                    new_pfd['dep'] = df.columns[dep]
                    new_pfd['tableau'] = TPs
                    new_pfd['vios'] = []
                    # for ii in range(len(new_pfd['vios'])):
                    #     new_pfd['vios'].remove(new_pfd['vios'][0])
                    if len(vio_idx) < 30 and len(vio_idx) > 0:
                        vrec_df = pd.DataFrame(df.loc[list(vio_idx)])
                        vrec_df.sort_index(inplace=True)
                        new_pfd['vios'] = vrec_df
                    results['pfds'].append(new_pfd)
                # if valid_pfd:
                #     generalized = check_for_generalization(TPs, gms, dets[0], dep, df)
                #     print(tab_name+'::'+df.columns[dets]+'::'+df.columns[dep], '==>', generalized)
                #     fout.write(f + "::"+ df.columns[dets[0]] + "<==>" + df.columns[dep[0]] + "\n")
                #     for attrib in dets:
                #         atts_to_be_removed.append(attrib)

            new_cols_list = []
            for ncl in range(len(new_cols_list)):
                new_cols_list.remove(new_cols_list[0])
            for att_id in cols_list:
                if att_id in atts_to_be_removed:
                    continue
                new_cols_list.append(att_id)
            cols_list = new_cols_list
    return results




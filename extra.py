from pfd import *
import json
import requests
import re
import random
from uszipcode import SearchEngine

# get the precision and recall based on the ground truth
def get_P_R(param_config):
    data = []
    for dd in range(len(data)):
        data.remove(data[0])
    cols = ["dataset", "# dep", "Precision (P)", "Recall (R)"]
    if (param_config["repo"] == "DGOV"):
        i = 1
    if (param_config["repo"] == "CHE"):
        i = 6
    if (param_config["repo"] == "MIT"):
        i = 11
    data_dir = os.path.join(param_config["data_dir"])
    data_files = get_csv_fnames_list(data_dir)
    results_dir = os.path.join(param_config["results_main_dir"], param_config["repo"])
    dep_fname = os.path.join(results_dir, param_config["repo"] + "_Deps.txt")
    
    tab_dict = dict()
    tab_dict.clear()
    for f in data_files:
        tab_name = os.path.join(data_dir, f)
        tab_id = "T"+str(i)
        tab_dict[tab_id] = f
        i += 1
    gt1 = os.path.join(param_config["results_main_dir"], "ground_truth")
    gt = os.path.join(gt1, param_config["repo"] + "_GT.txt")
    with open(dep_fname) as fid:
        try:
            lines_res = fid.readlines()
        except e:
            print(e)    

    with open(gt) as gt_fid:
        try:
            lines_gt = gt_fid.readlines()
        except e:
            print(e)
    for t in tab_dict:
        correct = 0
        for line in lines_res:
            if tab_dict[t] in line:
                if (line in lines_gt):
                    correct += 1
                
        tot_pfds = 0
        for l_res in lines_res:
            if l_res.startswith(tab_dict[t]):
                tot_pfds += 1
        gt_pfds = 0
        for l_gt in lines_gt:
            if l_gt.startswith(tab_dict[t]):
                gt_pfds += 1
        P = correct / tot_pfds * 100 
        R = correct / gt_pfds * 100
        data.append([tab_dict[t], tot_pfds, P, R])
    results_df = pd.DataFrame(data, columns=cols)
    return results_df






def find_specific_pfds(param_config):
    data_file = param_config["fname"]
    output_fname = param_config["det"] + "__" + param_config["dep"] + ".csv"
    results_file = os.path.join(param_config["results_main_dir"], output_fname)
    results_dir = param_config["results_main_dir"]
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    start_time = time.time()
    df = read_table(data_file)
    cols_len = len(df.columns)
    init_cols_list = range(cols_len)
    output_args = {
            "tname" : output_fname,
            "db" : 0,
            "data" : df,
            "dir_name" : results_dir
        }
    max_num_dets = 1
    lvl = 0
    df_details = get_df_details(df)
    df_details = tokens_vs_ngrams_df(df, df_details)
    gms, cand_cols_list, df_details = create_gms_dict_tab(df, df_details)
    rev_gms = create_rev_gms_dict_tab(df, df_details, cand_cols_list)
    
    dep = param_config["dep_id"]
    det = param_config["det_id"]
    
    TPs = []
    vio_idx = []
    if lvl == 0:
        det_col = df.columns[det]
        dep_col = df.columns[dep]
        A = df[det_col[0]]
        B = df[dep_col[0]]
        TPs, vio_idx = find_substring_dependency(A, det[0], B, dep[0], df_details)
    if len(TPs) == 0:
        dets_gms = extract_part_of_dict(det, gms)
        dets_rev_gms = extract_part_of_dict(det, rev_gms)
        dep_gms = extract_part_of_dict(dep, gms)
        dep_rev_gms = extract_part_of_dict(dep, rev_gms)
        TPs, vio_idx = pfd_discovery(dets_gms, dep_gms, dep_rev_gms, B, param_config)
    output_args["dets"] = det
    output_args["dep"] = dep

    valid_pfd = output_TPs_and_vios(output_args, TPs, vio_idx, param_config["min_acceptable_coverage"])
    end_time = time.time()
    spent_time = end_time - start_time
    print("Time spent to process the dependency ({0:s}) ==> ({1:s}) is ({2:s})".format(det_col[0], dep_col[0], str(spent_time)))
    if output_args["db"]:
        out_dir = os.path.join(output_args["dir_name"], output_args["tname"] + '/')
    else:
        out_dir = os.path.join(output_args["dir_name"], get_fname(output_args["tname"]) + '/')
    
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
#     print(tps_fname)
    return tps_fname
    
    


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






    
    
    
def get_gender_by_name(api_url_base, name, key=""):

    api_url = '{0}{1}{2}'.format(api_url_base, name, key)

    response = requests.get(api_url)

    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def validate_name_gender_pfds(pfds_file_name):
    first_names = []
    for ii in range(len(first_names)):          #make sure the list is empty
        first_names.remove(first_names[0])
    with open(pfds_file_name, "r") as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith("==") or line.startswith("Coverage"):
            continue
        else:
            toks1 = re.split('(\d*\.\d+|\W)', line)
            toks = [t for t in toks1 if len(t) > 0]
            gender = line.rsplit("', '", 2)
            if toks[2] == ',':
                first_names.append([toks[4], gender[1][0]])
            else:
                first_names.append([toks[2], gender[1][0]])
    url_base = 'https://api.genderize.io/?name='
    validated_records = []
    for i in range(len(validated_records)):
        validated_records.remove[validated_records[0]]
    for record in first_names:
        name = record[0]
        gender = record[1]
        gender_info = get_gender_by_name(url_base, name)
        gender_info["org_name"] = name
        gender_info["org_gender"] = gender
        validated_records.append(gender_info)
        
    correct = 0
    incorrect = 0
    for record in validated_records:
        if (record["probability"] < 0.9):
            incorrect += 1
            error_pfd = "(" + record["org_name"]+ ") is used for ("+ record["gender"]
            error_pfd += ") with probability = " + str(record["probability"])
            print(error_pfd)
        else:
            correct += 1
    precision = correct / (correct + incorrect) * 100
    print("Correct = ", correct, "   Incorrect = ", incorrect, "error rate = ", precision, "%")
    
    
    
def validate_fax_state_pfds(pfds_file_name):
    df_GT = read_table("../data/pfd_validation/US_Phone_Code.csv")
    di = dict()
    di.clear()
    idxs = df_GT.index.tolist()
    for idx in idxs:
        a = df_GT[df_GT.columns[2]][idx]
        b = a.split(',\xa0')
        for el in b:
            if el in di:
                di[el].append(df_GTdf_GTdf_GT[df_GTdf_GT.columns[1]][idx])
                print("duplicate code: ", el)
            else:
                di[el] = [df_GT[df_GT.columns[1]][idx]]
    correct = 0
    incorrect = 0
    with open(pfds_file_name, "r") as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith("==") or line.startswith("Coverage"):
            continue
        else:
            s1 = line.rsplit('::', 1)
            s2 = re.split('(\d*\.\d+|\W)', s1[0])
            ss = [t for t in s2 if len(t) > 0]
            rec_state = ss[len(ss) - 1]
            for ii in ss:
                if ii.isspace():
                    continue
                if (ii[0].isdigit()):
                    fax = ii
                    break
    #         print(fax, "<==>", state)
            if (fax in di):
                true_state = di[fax]
                if not(true_state[0] == rec_state):
                    print(fax, rec_state, "|=", true_state)
                    incorrect += 1
                else:
                    correct += 1
    error_rate = incorrect / (correct + incorrect)*100
    accuracy = 100 - error_rate
    print("Correct = ", correct, "\tIncorrect = ", incorrect, 
          "error rate = {0:.2f}".format(error_rate)+ "%", 
          "\tAccuracy = {0:.2f}".format(accuracy), "%")

    
def validate_zip_city_pfds(pfds_file_name):    
    correct = 0
    incorrect = 0
    code_city_dict = dict()
    code_city_dict.clear()
    search = SearchEngine(simple_zipcode=False)
    with open(pfds_file_name, "r") as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith("==") or line.startswith("Coverage"):
            continue
        else:
            s1 = line.rsplit('::', 1)
            s2 = re.split('(\d*\.\d+|\W)', s1[0])
            ss = [t for t in s2 if len(t) > 0]
            city1 = s1[0].rsplit('\'', 1)
            rec_city = city1[1]
            for ii in ss:
                if ii.isspace():
                    continue
                if (ii[0].isdigit()):
                    Zip = ii
                    break
            zipcode = search.by_zipcode(Zip)
            true_city = zipcode.major_city
            if not(true_city.lower() == rec_city.lower()):
                print(Zip, rec_city, "|=", true_city)
                incorrect += 1
            else:
                correct += 1
    error_rate = incorrect / (correct + incorrect)*100
    accuracy = 100 - error_rate
    print("Correct = ", correct, "\tIncorrect = ", incorrect, 
          "error rate = {0:.2f}".format(error_rate)+ "%", 
          "\tAccuracy = {0:.2f}".format(accuracy), "%")

    
    
def add_noise_active_domain(tab_name, noise_size):
    vio_data = []
    for i in range(len(vio_data)):
        vio_data.remove(vio_data[0])
    
    df = read_table(tab_name)

    cur_states = df['State'].unique()
    


    noise = round(noise_size*len(df))
    n_idxs = df.sample(noise).index.tolist()
    new_df = df
    data_cols = ['idx','old','new']
    for i in n_idxs:
        while True:
            new_state = str(random.choice(cur_states))
            if not(new_state == str(df['State'][i])):
                vio_data.append([str(i), str(df['State'][i]),str(new_state)])
                new_df['State'][i] = new_state
                break
    vios = pd.DataFrame(vio_data, columns = data_cols)
    return new_df, vios


def add_noise_inactive_domain(tab_name, states, noise_size):
    vio_data = []
    for i in range(len(vio_data)):
        vio_data.remove(vio_data[0])
    
    df = read_table(tab_name)

    cur_states = df['State'].unique()
    all_states_df = read_table(states)
    all_states = all_states_df[all_states_df.columns[1]]
    states_diff = list(set(all_states) - set(cur_states))
    states_diff_df = pd.DataFrame(states_diff, columns=['Abbr'])


    noise = round(noise_size*len(df))
    n_idxs = df.sample(noise).index.tolist()
    new_df = df
    data_cols = ['idx','old','new']
    for i in n_idxs:
        new_state = str(states_diff_df.sample(1).iloc[0]['Abbr'])
        vio_data.append([str(i), str(df['State'][i]),str(new_state)])
        new_df['State'][i] = new_state
    vios = pd.DataFrame(vio_data, columns = data_cols)
    return new_df, vios


def find_pfds_for_exp3(df, param_config):
    start_time = time.time()
    cols_len = len(df.columns)
    init_cols_list = range(cols_len)
    max_num_dets = 1
    lvl = 0
    df_details = get_df_details(df)
    df_details = tokens_vs_ngrams_df(df, df_details)
    gms, cand_cols_list, df_details = create_gms_dict_tab(df, df_details)
    rev_gms = create_rev_gms_dict_tab(df, df_details, cand_cols_list)
    
    dep = param_config["dep_id"]
    det = param_config["det_id"]
    
    TPs = []
    vio_idx = []
    if lvl == 0:
        det_col = df.columns[det]
        dep_col = df.columns[dep]
        A = df[det_col[0]]
        B = df[dep_col[0]]
        TPs, vio_idx = find_substring_dependency(A, det[0], B, dep[0], df_details)
    if len(TPs) == 0:
        dets_gms = extract_part_of_dict(det, gms)
        dets_rev_gms = extract_part_of_dict(det, rev_gms)
        dep_gms = extract_part_of_dict(dep, gms)
        dep_rev_gms = extract_part_of_dict(dep, rev_gms)
        TPs, vio_idx = pfd_discovery(dets_gms, dep_gms, dep_rev_gms, B, param_config)
    
    return vio_idx



def error_detection(params_config):
    noise = range(10)
    num_runs = 10
    data = []
    for ii in range(len(data)):
        data.remove(data[0])
    tab_name = params_config["fname"]
    for no in noise:
        err = (1 + no) / 100
        avg_PL = []
        avg_RL = []
        for ii in range(len(avg_PL)):
            avg_PL.remove(avg_PL[0])
        for ii in range(len(avg_RL)):
            avg_RL.remove(avg_RL[0])
        for i in range(num_runs):
            if (params_config["active"] == "Y"):
                ddf, vios = add_noise_active_domain(tab_name, err)
            else:
                states_file = params_config["statesfname"]
                ddf, vios = add_noise_inactive_domain(tab_name, states_file, err)
            vio_idx = find_pfds_for_exp3(ddf, params_config)
            errors_idx_set = list(vios['idx'])
    #         errors_idx_set = set(errors_idx['idx'])
            correct = 0
            for vidx in vio_idx:
                if str(vidx) in errors_idx_set:
                    correct += 1
            if not (len(vio_idx) == 0):
                avg_PL.append(correct / len(vio_idx))
            if not (len(vio_idx) == 0):
                avg_RL.append(correct / len(errors_idx_set))
    #         print(err, avg_P, avg_R)
        p_sorted = sorted(avg_PL, reverse=True)
        r_sorted = sorted(avg_RL, reverse=True)
        avg_P = sum(p_sorted) / float(num_runs)
        avg_R = sum(r_sorted) / float(num_runs)
        data.append([err, avg_P, avg_R])
    cols = ["Error Rate", "Precision", "Recall"]
    df = pd.DataFrame(data, columns = cols)
    return df
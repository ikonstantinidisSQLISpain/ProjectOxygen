import json
from pathlib import Path
import numpy as np

def load_json(path: str | Path) -> dict:
    """
    Loads a JSON file and return its contents as a Python dictionary.

    Args:
        path: Path to the JSON file.

    Returns:
        A dictionary containing the parsed JSON.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file does not contain valid JSON.
        TypeError: If the top-level JSON object is not a dictionary or list.
    """
    path = Path(__file__).parent / path
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data



def print_keys(data):
    for k,v in load_json("./RawDataStructure/whoz__certification_report_anonymized_structure.json").items():
        if isinstance(v, dict):
            print(k, "dict")
        else:
            print(k, v)
    return None

files_paths = [
    "./RawData/whoz__certification_report_anonymized.json",
    "./RawData/whoz__profile_report_anonymized.json",
    "./RawData/whoz__skill_report_anonymized.json",
    "./RawData/whoz__talent_report_anonymized.json",
    "./RawData/whoz__user_report_anonymized.json"
]


whoz = {
    "cert": load_json(files_paths[0]),
    "prof": load_json(files_paths[1]),
    "skil": load_json(files_paths[2]),
    "tale": load_json(files_paths[3]),
    "user": load_json(files_paths[4])
}

"""
talentId str
title str
workspaceId str
qualificationIds_list dict
attachment dict
attachmentId str

# attachment and attachmentId seems to be related, we check that
description str
endDate str
expirationDate str
startDate str
"""

def check_certs():

    certs = whoz["cert"]
    matches = list()
    for cert in certs:
        try:
            id_1 = cert["attachment"]["uid"]
            id_2 = cert["attachmentId"]
            matches.append(id_1 == id_2)
        except KeyError:
            continue # Not all have this key

    print(all(matches))


    return None

#check_certs()


def checks_user():

    # A totally sane function with a normal amount of set creations
    # There are not enough unnecessary O(n) iterations. (Sarcasm)

    users = whoz["user"]
    matches_created = list()
    unique_created = list()
    matches_last_mod = list()
    unique_last_mod = list()
    users_ids = [user["id"] for user in users]
    for user in users:
        created_by = user["createdBy"]
        last_mod_by = user["lastModifiedBy"]
        if created_by in users_ids:
            matches_created.append(created_by)
        else:
            unique_created.append(created_by)

        if last_mod_by in users_ids:
            matches_last_mod.append(last_mod_by)
        else:
            unique_last_mod.append(last_mod_by)

    print(unique_last_mod)
    print(unique_created)
    # Apparently all of these are "null"
    print(set(unique_created))
    print(set(unique_last_mod))

    len_created = len(set(matches_created))
    len_last_mod = len(set(matches_last_mod))
    len_no_created = len(set(unique_created))
    len_no_mod = len(set(unique_last_mod))


    print(len_created, len_last_mod, len_no_created, len_no_mod, len(users_ids))

    print("Unique Created Not in Unique No Mod")
    print([cre for cre in set(unique_created) if cre not in set(unique_last_mod)])
    print("Unique No Mod Not in Unique Create")
    print([cre for cre in set(unique_last_mod) if cre not in set(unique_created)])

    return None


#checks_user()


def checks2_user():
    """Here we check that all the list for federationRoles, workspaceRoles and agenticStudioRoles are empty."""

    users = whoz["user"]
    max_len_work = 0
    counter_work = 0

    max_len_fed = 0
    counter_fed = 0

    max_ag = 0
    counter_ag = 0
    for user in users:
        if isinstance(user["workspaceRoles"], list):
            max_len_work = max(max_len_work, len(user["workspaceRoles"]))
            counter_work += 1

        if isinstance(user["federationRoles"], list):
                    max_len_fed = max(max_len_fed, len(user["federationRoles"]))
                    counter_fed += 1

        if isinstance(user["agenticStudioRoles"], list):
                            max_ag = max(max_ag, len(user["agenticStudioRoles"]))
                            counter_ag += 1

    print(max_len_work, counter_work)
    print(max_len_fed, counter_fed)
    print(max_ag, counter_ag)
    print(len(users))
    
    return None


#checks2_user()


def checks3_user():
    """Checks that the id that is used to reference the workspace and federation matches the one typed in workspaceID or federationID"""

    users = whoz["user"]

    w_not_matches = list()
    f_not_matches = list()

    workspace_ids = set()
    federation_ids = set()

    for user in users:
        if isinstance(user["workspaceRoles"], dict):
            for k,v in user["workspaceRoles"].items():
                workspace_ids.add(k)
                if v["workspaceId"] != k:
                    w_not_matches.append((k, v["workspaceId"]))

        if isinstance(user["federationRoles"], dict):
            for k,v in user["federationRoles"].items():
                federation_ids.add(k)
                if v["federationId"] != k:
                    f_not_matches.append((k, v["federationId"]))

    if len(w_not_matches) > 0:
        print(w_not_matches)
    else:
        print("workspace Id matches its key")

    if len(f_not_matches) > 0:
        print(f_not_matches)
    else:
        print("federation Id matches its key")


    matches_fed_work = list()
    unmatches_fed_work = list()

    for fed in federation_ids:
        if fed in workspace_ids:
            matches_fed_work.append(fed)
        else:
            unmatches_fed_work.append(fed)

    print(len(federation_ids), len(workspace_ids))
    print(len(matches_fed_work), len(unmatches_fed_work))

    return None


#checks3_user()


def check_skill():

    skills = whoz["skil"]

    unique_id = set()

    unique_type = set()
    unique_gran = set()
    unique_removed = set()
    unique_nature = set()

    counter_not_gran = 0
    counter_not_type = 0
    counter_not_removed = 0
    counter_not_nature = 0

    for skill in skills:
        unique_id.add(skill["id"])

        try:
            unique_type.add(skill["type"])
        except KeyError:
            counter_not_type += 1
        try:
            unique_gran.add(skill["granularity"])
        except KeyError:
            counter_not_gran += 1
        try:
            unique_removed.add(skill["removed"])
        except KeyError:
            counter_not_removed += 1
        try:
            unique_nature.add(skill["nature"])
        except KeyError:
            counter_not_nature += 1

    if len(skills) == len(unique_id):
        print("There is a row for each skill.")
    else:
        print("A skill may have different data.")

    print(unique_type)
    print(unique_gran)
    print(unique_removed)
    print(unique_nature)

    print(len(skills), 
          counter_not_type, 
          counter_not_removed, 
          counter_not_gran, 
          counter_not_nature)

    return None


#check_skill()


def check2_skill():

    skills = whoz["skil"]

    unique_id = set()
    for skill in skills:
        unique_id.add(skill["id"])

    counter = 0
    missing_parents = list()
    missing_key_counter = 0

    for skill in skills:
        try:
            if skill["parentId"] not in unique_id:
                counter += 1
                missing_parents.append(skill["parentId"])
        except KeyError:
            missing_key_counter += 1

    print(len(skills), missing_key_counter)
    print(len(missing_parents))
    print(set(missing_parents))
    print("null" in unique_id)
    print(None in unique_id)
    return None


#check2_skill()

def get_quantiles(data):
    quants = [np.quantile(data, n) for n in [0.25, 0.5, 0.75]]
    return [min(data), max(data), *quants]

def div(num,denom):
    if denom == 0:
        return 0
    else:
        return num/denom

def check3_skill():

    skills = whoz["skil"]
    
    unique_id = set()
    for skill in skills:
        unique_id.add(skill["id"])

    unique_also = set()

    
    parent_in_alsos_counter = 0
    parent_in = list()
    for skill in skills:
        alsos = skill["alsoPartOf"]

        try:
            cond = skill["parentId"] in alsos
            if cond:
                parent_in_alsos_counter += 1
                parent_in.append(skill["id"])
        except KeyError:
            pass
        for also in alsos:
            unique_also.add(also)


    unique_also_in = set()
    unique_also_not_in = set()
    counter_also_in = 0
    counter_also_not_in = 0


    for also in unique_also:
        if also in unique_id:
            counter_also_in += 1
            unique_also_in.add(also)
        else:
            counter_also_not_in += 1
            unique_also_not_in.add(also)


    print(len(skills))
    print(parent_in_alsos_counter, parent_in)
    print(counter_also_in, counter_also_not_in)

    in_ratios = list()
    
    for skill in skills:
        alsos = skill["alsoPartOf"]
        n_alsos = len(alsos)
        if n_alsos == 0:
            continue
        counter_in = 0
        for also in alsos:
            if also in unique_id:
                counter_in += 1
        in_ratios.append(div(counter_in,n_alsos))

    
    print(len(alsos))
    print(get_quantiles(in_ratios))
    print(len([ratio for ratio in in_ratios if ratio < 1]))

    cond = False
    for skill in skills:
        alsos = skill["alsoPartOf"]
        for also in alsos:
            if also in unique_id:
                cond = True
                break
        if cond:
            break
    print(skill["id"], also)


    return None


#check3_skill()


def cert_check():


    certs = whoz["cert"]

    unique_certs = set()

    att_match_attid = list()

    attid_exist_but_not_att_counter = 0

    for cert in certs:
        unique_certs.add(cert["id"])
        att_exi = True
        attid_exi = True
        try:
            att = cert["attatchment"]
        except KeyError:
            att_exi = False

        try:
            attid = cert["attatchmentId"]
        except KeyError:
            attid_exi = False

        if attid_exi and not att_exi:
            attid_exist_but_not_att_counter += 1

        if attid_exi and attid_exi:
            att_match_attid.append(cert["attatchment"]["uid"] == cert["attatchmentId"])


    print(attid_exist_but_not_att_counter)
    print(len(certs) == len(unique_certs))
    print(all(att_match_attid))

    return None


#cert_check()


def cert2_check():

    certs = whoz["cert"]
    users = whoz["user"]

    users_ids = [user["id"] for user in users]

    created_certs = dict()
    last_mod_cert = dict()

    created_counter = 0
    last_mod_counter = 0
    not_created_counter = 0
    not_last_mod_counter = 0
    null_created_counter = 0
    null_last_mod_counter = 0
    for cert in certs:
        try:
            if cert["createdBy"] is None:
                null_created_counter += 1
            if cert["createdBy"] in users_ids:
                created_counter += 1
        except KeyError:
            not_created_counter += 1

        try:
            if cert["lastModifiedBy"] is None:
                null_last_mod_counter += 1
            if cert["lastModifiedBy"] in users_ids:
                last_mod_counter += 1
        except KeyError:
            not_last_mod_counter += 1

        try:
            user_cr = cert["createdBy"]
            try:
                created_certs[user_cr]
            except KeyError:
                created_certs[user_cr] = list()

            created_certs[user_cr].append(cert["id"])
        except KeyError:
            pass

        try:
            user_lm = cert["lastModifiedBy"]
            try:
                last_mod_cert[user_lm]
            except KeyError:
                last_mod_cert[user_lm] = list()

            last_mod_cert[user_lm].append(cert["id"])
        except KeyError:
            pass


    print(len(certs))
    print(created_counter, last_mod_counter)
    print(not_created_counter, not_last_mod_counter)
    print(null_created_counter, null_last_mod_counter)

    cr_gt1_counter = 0
    lm_gt1_counter = 0
    for user, certs_ids in created_certs.items():
        if len(certs_ids) > 1:
            cr_gt1_counter += 1

    for user, certs_ids in last_mod_cert.items():
        if len(certs_ids) > 1:
            lm_gt1_counter += 1

    print(len(created_certs.keys()), cr_gt1_counter)
    print(len(last_mod_cert.keys()), lm_gt1_counter)
        
    return None


#cert2_check()


def cert3_check():

    certs = whoz["cert"]

    prof_talent = dict()
    talent_prof = dict()

    for cert in certs:
        try:
            prof_talent[cert["profileId"]]
        except KeyError:
            prof_talent[cert["profileId"]] = set()

        prof_talent[cert["profileId"]].add(cert["talentId"])

        try:
            talent_prof[cert["talentId"]]
        except KeyError:
            talent_prof[cert["talentId"]] = set()

        talent_prof[cert["talentId"]].add(cert["profileId"])


    counter_prof = 0
    for prof, talents in prof_talent.items():
        if len(talents) > 1:
            counter_prof += 1

    counter_tale = 0
    for talent, profiles in talent_prof.items():
        if len(profiles) > 1:
            counter_tale += 1

    print(len(prof_talent.keys()), len(talent_prof.keys()))
    print(counter_prof, counter_tale)

    sub_dict = {k:v for k,v in talent_prof.items() if len(v) > 1}

    #print(sub_dict)
    return None

#cert3_check()

def cert4_check():

    certs = whoz["cert"]
    
        talent_prof = dict()
        prof_cert = dict()
    
        for cert in certs:
            try:
                prof_cert[cert["profileId"]]
            except KeyError:
                prof_cert[cert["profileId"]] = set()
    
            prof_cert[cert["profileId"]].add(cert["id"])
    
            try:
                talent_prof[cert["talentId"]]
            except KeyError:
                talent_prof[cert["talentId"]] = set()
    
            talent_prof[cert["talentId"]].add(cert["profileId"])


        talent_certs_counts = dict()
        for tal, profs in talent_prof.items():
            tal_prof_certs = [prof_cert[pro] for pro in profs]

            data = dict()
            ini_cert_list = tal_prof_certs[0]
            for i, cert_list in enumerate(tal_prof_certs):
                if i == 0:
                    continue
                

    

    return None

def check_profile():
    profiles = whoz["prof"]

    unique_ids = set()

    for prof in profiles:
        unique_ids.add(prof["id"])

    if len(profiles) == len(unique_ids):
        print("JSON file only has data abaout profiles. One row per profile.")
    else:
        print("A single profile may have different data.")
    return None

#check_profile()

def check2_profile():

    profiles = whoz["prof"]

    sched_counter = {
        "empty": 0,
        "missing": 0
    }

    fd_counter = {
        "empty": 0,
        "missing": 0
    }

    cf_counter = {
        "empty": 0,
        "missing": 0
    }

    tfd_counter = {
        "empty": 0,
        "missing": 0
    }

    tsr_counter = {
        "empty": 0,
        "missing": 0
    }

    ts_counter = {
        "empty": 0,
        "missing": 0
    }

    for prof in profiles:
        try:
            if len(prof["customFields"]) == 0:
                cf_counter["empty"] += 1
        except KeyError:
            cf_counter["missing"] += 1

        try:
            if len(prof["functionalDomains"]) == 0:
                fd_counter["empty"] += 1
        except KeyError:
            fd_counter["missing"] += 1

        try:
            if len(prof["targetFunctionalDomains"]) == 0:
                tfd_counter["empty"] += 1
        except KeyError:
            tfd_counter["missing"] += 1

        try:
            if len(prof["targetSkillRatings"]) == 0:
                tsr_counter["empty"] += 1
        except KeyError:
            tsr_counter["missing"] += 1

        try:
            if len(prof["targetSkills"]) == 0:
                ts_counter["empty"] += 1
            else:
                print(prof["id"], prof["targetSkills"])
                # We print cause it is only one
        except KeyError:
            ts_counter["missing"] += 1

        try:
            if len(prof["schedules"]) == 0:
                sched_counter["empty"] += 1
        except KeyError:
            sched_counter["missing"] += 1

    print(len(profiles))
    print(cf_counter["empty"], cf_counter["missing"], cf_counter["empty"] + cf_counter["missing"])
    print(fd_counter["empty"], fd_counter["missing"], fd_counter["empty"] + fd_counter["missing"])
    print(tfd_counter["empty"], tfd_counter["missing"], tfd_counter["empty"] + tfd_counter["missing"])
    print(tsr_counter["empty"], tsr_counter["missing"], tsr_counter["empty"] + tsr_counter["missing"])
    print(ts_counter["empty"], ts_counter["missing"], ts_counter["empty"] + ts_counter["missing"])
    print(sched_counter["empty"], sched_counter["missing"], sched_counter["empty"] + sched_counter["missing"])

    
    return None


#check2_profile()


def check3_profile():

    profiles = whoz["prof"]

    is_list_empty_counter = 0
    is_list_not_empty_counter = 0
    is_not_list_counter = 0
    for prof in profiles:
        if isinstance(prof["completionDetails"], list):
            if len(prof["completionDetails"]) == 0:
                is_list_empty_counter += 1
            else:
                is_list_not_empty_counter += 1
        else:
            is_not_list_counter += 1

    print(len(profiles))
    print(is_list_empty_counter, is_list_not_empty_counter, is_list_empty_counter + is_list_not_empty_counter)
    print(is_not_list_counter, is_not_list_counter + is_list_empty_counter + is_list_not_empty_counter)

    return None


#check3_profile()

def check4_profile():

    profiles = whoz["prof"]

    unique_talents = set()

    for prof in profiles:
        unique_talents.add(prof["talentId"])

    print(len(profiles))
    print(len(unique_talents))

    return None

#check4_profile()

def check5_profile():

    profiles = whoz["prof"]
    certs = whoz["cert"]

    talent_profile_prof = dict()
    talent_profile_cert = dict()

    unique_ids = set()

    for prof in profiles:
        unique_ids.add(prof["id"])
        try:
            talent_profile_prof[prof["talentId"]]
        except KeyError:
            talent_profile_prof[prof["talentId"]] = set()

        talent_profile_prof[prof["talentId"]].add(prof["id"])

    for cert in certs:
        try:
            talent_profile_cert[cert["talentId"]]
        except KeyError:
            talent_profile_cert[cert["talentId"]] = set()

        talent_profile_cert[cert["talentId"]].add(cert["profileId"])


    profs_not_in_profiles_counters = list()
    for tal, profs in talent_profile_cert.items():
        counter = 0
        if len(profs) > 1:
            for prof in profs:
                if prof in unique_ids:
                    counter += 1
        profs_not_in_profiles_counters.append(counter)
    
    print(max(profs_not_in_profiles_counters))
    # 1, it means that the extra profiles that a talent
    # may have, is not in the profiles file
    return None

#check5_profile()

def check6_profile():

    profiles = whoz["prof"]

    unique_main = set()
    for prof in profiles:
        unique_main.add(prof["main"])

    print(unique_main)
    return None

#check6_profile()

def check7_profile():

    profiles = whoz["prof"]

    unique_vn = set()
    unique_permission = set()
    unique_rrs = set()
    unique_status = set()
    unique_tr = set()

    unique_headline_permission = set()
    for prof in profiles:
        unique_vn.add(prof["versionName"])
        unique_permission.add(prof["permissionScope"])
        try:
            unique_rrs.add(prof["resumeRelationStatus"])
        except KeyError:
            pass
        unique_status.add(prof["status"])
        unique_tr.add(prof["travelRange"])

        try:
            unique_headline_permission.add(
                prof["headline"]["permissionScope"]
            )
        except KeyError:
            pass

    #print(unique_vn)
    print(unique_permission)
    print(unique_rrs)
    print(unique_status)
    print(unique_tr)
    print(unique_headline_permission)
    return None

#check7_profile()

def check_if_skill_name_exist(skill_name):

    skills = whoz["skil"]
    for skill in skills:
        for name in skill["name"]:
            if skill_name == name["text"]:
                return True
    return False

def check8_profile():
    """This is a heavy function"""
    profiles = whoz["prof"]
    
    n_profiles = len(profiles)
    prof_with_skills_counter = 0

    matching_skills_rate = list()
    not_matching_skills = set()


    for prof in profiles:
        prof_skills = prof["skillRatings"]
        n_skills = len(prof_skills)
        if n_skills > 0:
            prof_with_skills_counter += 1
            matching_skills_counter = 0
            for sk in prof_skills:
                if check_if_skill_name_exist(sk["skill"]):
                    matching_skills_counter += 1
                else:
                    not_matching_skills.add(sk["skill"])
            matching_skills_rate.append(matching_skills_counter/n_skills)

    print(not_matching_skills)
    print(get_quantiles(matching_skills_rate))
    return None

#check8_profile()

def check9_profile():

    profiles = whoz["prof"]
    unique_skills = set()

    cond = False
    for prof in profiles:
        prof_skills = prof["skillRatings"]
        for sk in prof_skills:
            if sk["skill"] in unique_skills:
                cond = True
                print(f"Profile {prof['id']} has the skill {sk} that was already in another profile")
                break
            else:
                unique_skills.add(sk["skill"])
        if cond:
            break

    return None

#check9_profile()


def check10_profile():
    profiles = whoz["prof"]

    activity_profile = dict()
    activity_concept_profile = dict()

    match_profs_rates = list()
    match_talents_rates = list()

    for prof in profiles:
        aptitudes = prof["aptitudes"]
        n_apt = len(aptitudes)
        counter_prof = 0
        counter_talen = 0
        for apt in aptitudes:
            if apt["profileId"] == prof["id"]:
                counter_prof += 1
            if apt["talentId"] == prof["talentId"]:
                counter_talen += 1
            try:
                activity_profile[apt["id"]]
            except KeyError:
                activity_profile[apt["id"]] = set()

            activity_profile[apt["id"]].add(prof["id"])



            try:
                activity_concept_profile[apt["name"]]
            except KeyError:
                activity_concept_profile[apt["name"]] = set()

            
            activity_concept_profile[apt["name"]].add(prof["id"])
            

        if n_apt > 0:
            ratio_prof = div(counter_prof, n_apt)
            ratio_tal = div(counter_talen, n_apt)

            match_profs_rates.append(ratio_prof)
            match_talents_rates.append(ratio_tal)


    print(get_quantiles(match_profs_rates))
    print(get_quantiles(match_talents_rates))

    lengt2_counter = 0
    for act, profs in activity_profile.items():
        if len(profs) > 1:
            lengt2_counter += 1

    print(lengt2_counter)

    lengt2_counter_2 = 0
    for con, profs in activity_concept_profile.items():
        if len(profs) > 1:
            lengt2_counter_2 += 1

    print(lengt2_counter_2)

    return None


#check10_profile()

def check11_profile():

    profiles = whoz["prof"]

    concept_names = dict()

    concept_prof = dict()

    for prof in profiles:
        aptitudes = prof["aptitudes"]
        for apt in aptitudes:
            try:
                concept = apt["conceptId"]
            except KeyError:
                continue

            try:
                concept_names[concept]
            except KeyError:
                concept_names[concept] = set()

            concept_names[concept].add(apt["name"])

            try:
                concept_prof[concept]
            except KeyError:
                concept_prof[concept] = set()

            concept_prof[concept].add(prof["id"])

    counter = 0
    for con, names in concept_names.items():
        if len(names) > 1:
            counter += 1

    print(counter)

    counter_2 = 0
    for con, profs in concept_prof.items():
        if len(profs) > 1:
            counter_2 += 1

    print(counter_2)
    return None

#check11_profile()


def check12_profile():

    profiles = whoz["prof"]

    counter_sched = 0
    counter_qual = 0

    qual_prof = dict()
    for prof in profiles:
        if len(prof["schedules"]) > 1:
            counter_sched += 1

        if len(prof["qualificationIds"]) > 1:
            counter_qual += 1
            for qual in prof["qualificationIds"]:
                try:
                    qual_prof[qual]
                except KeyError:
                    qual_prof[qual] = set()
                qual_prof[qual].add(prof["id"])

    print(counter_sched, counter_qual)

    counter = 0
    for qual, profs in qual_prof.items():
        if len(profs) > 1:
            counter += 1

    print(counter)

    return None


#check12_profile()




def check13_profile():

    profiles = whoz["prof"]

    full_prof_matches = 0
    full_aps_matches = 0
    full_cf_matches = 0
    full_q_matches = 0

    actual_full_aps_matches = 0
    actual_full_qs_matches = 0

    cases_where_pos_has_unique_aps = 0
    cases_where_pos_has_unique_qs = 0

    unique_qs = set()
    
    for prof in profiles:
        cf = prof["customFields"]
        aps = [ap["id"] for ap in prof["aptitudes"]]
        quals = prof["qualificationIds"]

        positions = prof["positions"]

        n_positions = len(positions)
        prof_matches = 0
        aps_matches = 0
        cf_matches = 0
        q_matches = 0

        pos_aptitudes = set()
        pos_qs = set()
        for pos in positions:
            aps_pos = [ap["aptitudeId"] for ap in pos["aptitudeReferences"]]

            pos_aptitudes.update(aps_pos)

            cf_pos = pos["customFields"]
            quals_pos = pos["qualificationIds"]

            pos_qs.update(quals_pos)

            if pos["profileId"] == prof["id"]:
                prof_matches += 1

            if aps_pos == aps:
                aps_matches += 1
            if cf_pos == cf:
                cf_matches += 1
            if quals_pos == quals:
                q_matches += 1

        if prof_matches == n_positions:
            full_prof_matches += 1
        if aps_matches == n_positions:
            full_aps_matches += 1
        if cf_matches == n_positions:
            full_cf_matches += 1
        if q_matches == n_positions:
            full_q_matches += 1

        if list(pos_aptitudes) == aps:
            actual_full_aps_matches += 1
        if list(pos_qs) == quals:
            actual_full_qs_matches += 1

        for a in pos_aptitudes:
            if a not in aps:
                cases_where_pos_has_unique_aps += 1

        for q in pos_qs:
            if q not in quals:
                cases_where_pos_has_unique_qs += 1
                unique_qs.add(q)

    print(len(profiles))
    print(full_prof_matches)
    print(full_aps_matches)
    print(full_cf_matches)
    print(full_q_matches)
    print(actual_full_qs_matches)
    print(actual_full_aps_matches)
    print(cases_where_pos_has_unique_aps)
    print(cases_where_pos_has_unique_qs)
    print(unique_qs)
    print(len(unique_qs))




    return None

#check13_profile()


def check14_profile():

    profiles = whoz["prof"]
    users = whoz["user"]

    users_ids = [user["id"] for user in users]

    cr_counter = 0
    lu_counter = 0
    lm_counter = 0

    missing_cr = 0
    missing_lu = 0
    missing_lm = 0

    for prof in profiles:
        try:
            if prof["createdBy"] in users_ids:
                cr_counter += 1
        except KeyError:
            missing_cr += 1

        try:
            if prof["lastExplicitUpdateBy"] in users_ids:
                lu_counter += 1
            else:
                print(prof["lastExplicitUpdateBy"])
                
                # we can print cause it is only one
        except KeyError:
            missing_lu += 1

        try:
            if prof["lastModifiedBy"] in users_ids:
                lm_counter += 1
            else:
                print(prof["lastModifiedBy"])
        except KeyError:
            missing_lm += 1

    print(len(profiles))
    print(cr_counter, lu_counter, lm_counter)
    print(missing_cr, missing_lu, missing_lm)
    print(cr_counter + missing_cr,
          lu_counter + missing_lu,
          lm_counter + missing_lm)

    return None

#check14_profile()


def check15_profile():

    profiles = whoz["prof"]


    missing_parents_ratios = list()
    null_parents_ratios = list()
    parent_is_pos_ratios = list()
    parent_is_elsewhere_ratios = list()
    for prof in profiles:

        pos_ids = set()
        missing_parents = 0
        null_parents = 0

        parent_is_pos = 0
        parent_is_elsewhere = 0

        n_pos = len(prof["positions"])
        for pos in prof["positions"]:
            pos_ids.add(pos["id"])

        for pos in prof["positions"]:
            try:
                parent = pos["parentPositionId"]
                if parent is None:
                    null_parents += 1
                if parent in pos_ids:
                    parent_is_pos += 1
                else:
                    parent_is_elsewhere += 1
            except KeyError:
                missing_parents += 1

        if n_pos > 0:
            missing_parents_ratios.append(
                missing_parents / n_pos
            )
            null_parents_ratios.append(
                null_parents / n_pos
            )

            parent_is_pos_ratios.append(
                parent_is_pos / n_pos
            )

            parent_is_elsewhere_ratios.append(
                parent_is_elsewhere / n_pos
            )

    print(get_quantiles(missing_parents_ratios))
    print(get_quantiles(null_parents_ratios))
    print(get_quantiles(parent_is_pos_ratios))
    print(get_quantiles(parent_is_elsewhere_ratios))

    return None

#check15_profile()

def check_talent():

    talents = whoz["tale"]
    profiles = whoz["prof"]
    profiles_ids = [p["id"] for p in profiles]

    talents_id = set()

    prof_ids = set()
    prof_not_found = 0

    prof_is_in_profs = 0
    for tal in talents:
        talents_id.add(tal["id"])
        try:
            prof_ids.add(tal["profile"]["id"])
            if tal["profile"]["id"] in profiles_ids:
                prof_is_in_profs += 1
        except KeyError:
            prof_not_found += 1


    print(len(talents))
    print(len(talents_id))
    print(len(prof_ids))
    print(prof_not_found, len(prof_ids) + prof_not_found)
    print(prof_is_in_profs)

    # len talents matches talents_ids
    # There are 3 of them without profile
    # each talent has its onw profile, except those 3
    return None

#check_talent()

def check2_talent():

    certs = whoz["cert"]
    tals = whoz["tale"]

    tals_ids = [tal["id"] for tal in tals]

    unique_tals = set()


    counter = 0
    for cert in certs:
        unique_tals.add(cert["talentId"])
        if cert["talentId"] in tals_ids:
            counter += 1

    print(len(certs))
    print(len(tals_ids))
    print(len(unique_tals))
    print(counter)

    # All cert talents are in talents file

    return None

#check2_talent()

def check3_talent():

    talents = whoz["tale"]

    unique_permi = set()
    unique_ini = set()

    missing_ps = 0
    missing_ini = 0
    for tal in talents:
        try:
            unique_permi.add(tal["permissionScope"])
        except KeyError:
            missing_ps += 1
        try:
            unique_ini.add(tal["initials"])
        except KeyError:
            missing_ini += 1

    print(unique_permi)
    print(unique_ini)
    print(len(talents))
    print(missing_ps)
    print(missing_ini)
    return None

#check3_talent()

def get_initials_of_name(name):

    # Assumes that each part of the name is separated
    # by spaces

    ini_pos = [0]

    for i, letter in enumerate(name):
        if letter == ' ':
            ini_pos.append(i+1)

    initials = ''
    for i in ini_pos:
        initials = initials + name[i]

    return initials

def check4_talent():

    talents = whoz["tale"]
    users = whoz["user"]

    user_ids = [user["id"] for user in users]


    counter_user = 0
    counter_lm = 0
    counter_eu = 0
    counter_acdlu = 0

    missing_user = 0
    missing_lm = 0
    missing_eu = 0
    missing_acdlu = 0
    for tal in talents:
        try:
            if tal["userId"] in user_ids:
                counter_user += 1
        except KeyError:
            missing_user += 1

        try:
            if tal["lastModifiedBy"] in user_ids:
                counter_lm += 1
        except KeyError:
            missing_lm += 1

        try:
            if tal["endUser"] in user_ids:
                counter_eu += 1
        except KeyError:
            missing_eu += 1

        try:
            if tal["availabilityConfirmationDateLastUpdatedBy"] in user_ids:
                counter_acdlu += 1
        except KeyError:
            missing_acdlu += 1

    print(len(talents))
    print(counter_user, counter_lm, counter_eu, counter_acdlu)
    print(missing_user, missing_lm, missing_eu, missing_acdlu)
    print(
        counter_user + missing_user,
        counter_lm + missing_lm,
        counter_eu + missing_eu,
        counter_acdlu + missing_acdlu
    )

    return None


#check4_talent()

def get_user_name(user_id):

    users = whoz["user"]

    for user in users:
        if user["id"] == user_id:
            return user["username"]

    return None

def check5_talent():
    talents = whoz["tale"]
    users = whoz["user"]

    initials_provided_counter = 0
    user_provided = 0
    initials_and_user_provided = 0
    matching_initials = 0
    for tal in talents:
        try:
            tal["initials"]
            initials_provided = True
            initials_provided_counter += 1
        except KeyError:
            initials_provided = False
        try:
            username = get_user_name(tal["userId"])
            user_provided += 1
            if initials_provided:
                initials_and_user_provided += 1
                if get_initials_of_name(username) == tal["initials"]:
                    matching_initials += 1
        except KeyError:
            continue

    print(len(talents))
    print(initials_provided_counter)
    print(user_provided)
    print(initials_and_user_provided)
    print(matching_initials)
    return None

#check5_talent()

def check_list_match(l1, l2):

    if len(l1) != len(l2):
        return False

    for a in l1:
        if a not in l2:
            return False

    for a in l2:
        if a not in l1:
            return False

    return True

def check6_talent():

    talents = whoz["tale"]

    missing_qual = 0

    qual_matches = 0
    empty_cm = 0
    missing_cm = 0

    for tal in talents:
        try:
            qual = tal["qualificationIds"]
            qual_exist = True
        except KeyError:
            missing_qual += 1
            qual_exist = False

        try:
            pro = tal["profile"]
            pro_exist = True
        except KeyError:
            pro_exist = False

        if pro_exist and qual_exist:
            try:
                qual_pro = tal["profile"]["qualificationIds"]

                if check_list_match(qual, qual_pro):
                    qual_matches += 1
                else:
                    print(qual)
                    print(qual_pro)
                    print('-'*20)
            except KeyError:
                print(qual)
                print('*-'*20)

        try:
            if len(tal["customFields"]) == 0:
                empty_cm += 1
        except KeyError:
            missing_cm += 1


    print(len(talents))
    print(missing_qual)
    print(qual_matches)
    print(empty_cm)
    print(missing_cm)
    print(empty_cm + missing_cm)

    return None

#check6_talent()


def check7_talent():

    talents = whoz["tale"]

    counter_a = 0
    counter_mwh = 0
    counter_sd = 0
    counter_tag = 0

    unique_eu = set()

    missing_eu = 0
    missing_a = 0
    missing_mwh = 0
    missing_sd = 0
    missing_tag = 0
    for tal in talents:
        try:
            unique_eu.add(tal["endUser"])
        except KeyError:
            missing_eu += 1

        try:
            if len(tal["aspirations"]) > 0:
                counter_a += 1
        except KeyError:
            missing_a += 1

        try:
            if len(tal["maxWorkingHours"]) > 0:
                counter_mwh += 1
        except KeyError:
            missing_mwh += 1

        try:
            if len(tal["sharingDestinations"]) > 0:
                counter_sd += 1
        except KeyError:
            missing_sd += 1

        try:
            if len(tal["tags"]) > 0:
                counter_tag += 1
        except KeyError:
            missing_tag += 1

    print(len(talents))
    print(counter_a, missing_a, counter_a + missing_a)
    print(counter_mwh, missing_mwh, counter_mwh + missing_mwh)
    print(counter_sd, missing_sd, counter_sd + missing_sd)
    print(counter_tag, missing_tag, counter_tag + missing_tag)

    print(unique_eu)
    print(missing_eu)

    return None

#check7_talent()


def check8_talent():

    talents = whoz["tale"]
    profiles = whoz["prof"]
    users = whoz["user"]

    user_ids = [user["id"] for user in users]
    prof_ids = [prof["id"] for prof in profiles]
    tal_ids = [tal["id"] for tal in talents]

    counters = {
        'manager': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        },
        'mentor': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        },
        'external': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        }
    }
    for tal in talents:
        for id_name in ["manager", "mentor", "external"]:
            try:
                man = tal[f"{id_name}Id"]
                if man in user_ids:
                    counters[id_name]["user"] += 1
                elif man in prof_ids:
                    counters[id_name]["prof"] += 1
                elif man in tal_ids:
                    counters[id_name]["tal"] += 1
                else:
                    counters[id_name]["neither"] += 1
            except KeyError:
                counters[id_name]["missing"] += 1

    print(len(talents))
    for k,v in counters.items():
        print(k)
        sum_all = 0
        for k2, v2 in v.items():
            print('\t', k2, ':', v2)
            sum_all += v2
        print("\t", "sum", ':', sum_all)
    
    return None

#check8_talent()


def check9_talent():

    talents = whoz["tale"]

    missing_photo_counter = 0
    hists = {
        'name': dict(),
        'contentType': dict(),
        #'size': dict(),
        #'uid': dict(),
        'empty': dict()
    }
    for tal in talents:
        try:
            photo = tal["photo"]
            for key in hists.keys():
                try:
                    hists[key][photo[key]]
                except KeyError:
                    hists[key][photo[key]] = 0

                hists[key][photo[key]] += 1


        except KeyError:
            missing_photo_counter += 1

        

    for k, data in hists.items():
        print(k)
        for val, count in data.items():
            print('\t', val, ':', count)
    return None

#check9_talent()


def check10_talent():

    talents = whoz["tale"]

    empty_sd = 0
    missing_sd = 0
    empty_wsd = 0
    missing_wsd = 0
    recruit_is_list = 0
    recruit_is_not_list = 0
    for tal in talents:
        recruit = tal["recruitment"]
        if isinstance(recruit, list):
            recruit_is_list += 1
            print(recruit)
            continue
        recruit_is_not_list += 1
        try:
            if len(recruit["stageDates"]) == 0:
                empty_sd += 1
        except KeyError:
            missing_sd += 1

        try:
            if len(recruit["workflowStepDates"]) == 0:
                empty_wsd += 1
        except KeyError:
            missing_wsd += 1

    print(len(talents))
    print(recruit_is_list, recruit_is_not_list, recruit_is_not_list + recruit_is_list)
    print(empty_sd, empty_wsd)
    print(missing_sd, missing_wsd)
    print(
        empty_sd + missing_sd,
        empty_wsd + missing_wsd
    )

    return None

#check10_talent()

def check11_talent():

    talents = whoz["tale"]
    profiles = whoz["prof"]
    users = whoz["user"]

    user_ids = [user["id"] for user in users]
    prof_ids = [prof["id"] for prof in profiles]
    tal_ids = [tal["id"] for tal in talents]

    counters = {
        'sourcer': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        },
        'recruiter': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        }
    }
    for tal in talents:
        rec = tal["recruitment"]
        if isinstance(rec, list):
            continue
        for id_name in ["sourcer", "recruiter"]:
            try:
                man = rec[f"{id_name}Id"]
                if man in user_ids:
                    counters[id_name]["user"] += 1
                elif man in prof_ids:
                    counters[id_name]["prof"] += 1
                elif man in tal_ids:
                    counters[id_name]["tal"] += 1
                else:
                    counters[id_name]["neither"] += 1
            except KeyError:
                counters[id_name]["missing"] += 1

    print(len(talents))
    for k,v in counters.items():
        print(k)
        sum_all = 0
        for k2, v2 in v.items():
            print('\t', k2, ':', v2)
            sum_all += v2
        print("\t", "sum", ':', sum_all)

    return None

#check11_talent()

def check12_talent():

    talents = whoz["tale"]

    unique_scopes = set()
    missing_scopes_rates = list()

    for tal in talents:
        hist = tal["history"]
        missing_scopes = 0
        n_hist = len(hist)
        for h in hist:
            try:
                s = h["scope"]
                unique_scopes.add(s)
            except KeyError:
                missing_scopes += 1

        if n_hist > 0:
            missing_scopes_rates.append(missing_scopes/n_hist)

    print(len(talents))
    print(get_quantiles(missing_scopes_rates))
    print(unique_scopes)
    return None

#check12_talent()



def check13_talent():

    talents = whoz["tale"]
    profiles = whoz["prof"]
    users = whoz["user"]

    user_ids = [user["id"] for user in users]
    prof_ids = [prof["id"] for prof in profiles]
    tal_ids = [tal["id"] for tal in talents]

    counters = {
        'grade': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        },
        'role': {
            'user': 0,
            'prof': 0,
            'tal': 0,
            'missing': 0,
            'neither': 0
        }
    }
    for tal in talents:
        hist = tal["history"]
        for h in hist:
            for id_name in counters.keys():
                try:
                    man = h[f"{id_name}Id"]
                    if man in user_ids:
                        counters[id_name]["user"] += 1
                    elif man in prof_ids:
                        counters[id_name]["prof"] += 1
                    elif man in tal_ids:
                        counters[id_name]["tal"] += 1
                    else:
                        counters[id_name]["neither"] += 1
                except KeyError:
                    counters[id_name]["missing"] += 1

    print(len(talents))
    for k,v in counters.items():
        print(k)
        sum_all = 0
        for k2, v2 in v.items():
            print('\t', k2, ':', v2)
            sum_all += v2
        print("\t", "sum", ':', sum_all)

    return None


#check13_talent()

def mismatched_list(l1, l2, n1, n2):

    data = {
        n1: list(),
        n2: list()
    }

    for l in l1:
        if l not in l2:
            data[n1].append(l)

    for l in l2:
        if l not in l1:
            data[n2].append(l)

    return data

def check14_talent():

    tal_struct = load_json("./RawDataStructure/whoz__talent_report_anonymized_structure.json")
    prof_struct = load_json("./RawDataStructure/whoz__profile_report_anonymized_structure.json")

    data = mismatched_list(
                tal_struct["profile"].keys(), prof_struct.keys(),
                "talent", "profile"
            )

    for k, vals in data.items():
        print(k)
        for v in vals:
            print('\t', v)

    return None

#check14_talent()

def pretty_print_dict(data: dict, depth=0):

    for k, v in data.items():
        if isinstance(v, dict):
            print("\t"*depth, k, ':')
            pretty_print_dict(v, depth + 1)
        else:
            print("\t"*depth, k, ':', v)

    return None

def get_prof(prof_id):
    profiles = whoz["prof"]

    for pro in profiles:
        if pro["id"]:
            return pro

    raise KeyError("Error cause I don't want to redo")
    return pro

def check15_talent():

    talents = whoz["tale"]
    skills = whoz["skil"]
    profiles = whoz["prof"]

    skills_ids = [sk["id"] for sk in skills]

    main_skills_in_skills_rates = list()
    sec_skills_in_skills_rates = list()
    uncl_skills_in_skills_rates = list()


    counters = {
        'profile_not_provided': 0,
        'profile_not_exist': 0,
        'profile_exist': {
            'has_no_skills': 0,
            'main': {
                'is_not_provided': 0,
                'is_empty': 0,
                'all_in_skills': 0,
                'not_in_skills_counters': list()
            },
            'secondary': {
                'is_not_provided': 0,
                'is_empty': 0,
                'all_in_skills': 0,
                'not_in_skills_counters': list()
            },
            'unclassified': {
                'is_not_provided': 0,
                'is_empty': 0,
                'all_in_skills': 0,
                'not_in_skills_counters': list()
            }
        },
        'links': {
            'is_empty': 0,
            'is_not_provided': 0
        },
        'educations': {
            'is_empty': 0,
            'is_not_provided': 0
        },
        'main_sec_un_are_missing': 0
    }
    for t in talents:
        conditions = {
            "profile_not_provided": False,
            "profile_not_exist": False,
            "profile_has_no_skills": False,
            "main_is_not_provided": False,
            "sec_is_not_provided": False,
            "un_is_not_provided": False
        }
        try:
            t_pro = t["profile"]
        except KeyError:
            counters["profile_not_provided"] += 1
            conditions["profile_not_provided"] = True
            continue


        try:
            pro = get_prof(t_pro["id"])
        except KeyError:
            counters["profile_not_exist"] += 1
            conditions["profile_not_exist"] = True


        try:
            pro_sk = [sk["skill"] for sk in pro["skillRatings"]]
        except KeyError:
            counters["profile_exist"]["has_no_skills"] += 1
            conditions["profile_has_no_skills"] = True

        try:
            main = t_pro["mainSkills"]
            if len(main) == 0:
                counters["profile_exist"]["main"]["is_empty"] += 1
            else:
                print(main)
        except KeyError:
            counters["profile_exist"]["main"]["is_not_provided"] += 1
            conditions["main_is_not_provided"] = True

        try:
            sec = t_pro["secondarySkills"]
            if len(sec) == 0:
                counters["profile_exist"]["secondary"]["is_empty"] += 1
            else:
                print(sec)
        except KeyError:
            counters["profile_exist"]["secondary"]["is_not_provided"] += 1
            conditions["sec_is_not_provided"] = True

        try:
            unc = t_pro["unclassifiedSkills"]
            if len(unc) == 0:
                counters["profile_exist"]["unclassified"]["is_empty"] += 1
            else:
                print(unc)
        except KeyError:
            counters["profile_exist"]["unclassified"]["is_not_provided"] += 1
            conditions["un_is_not_provided"] = True

        if conditions["main_is_not_provided"] and conditions["sec_is_not_provided"] and conditions["un_is_not_provided"]:
            counters["main_sec_un_are_missing"] += 1

        try:
            links = t_pro["links"]
            if len(links) == 0:
                counters["links"]["is_empty"] += 1
            else:
                print(links)
        except KeyError:
            counters["links"]["is_not_provided"] += 1

        try:
            educ = t_pro["educations"]
            if len(educ) == 0:
                counters["educations"]["is_empty"] += 1
            else:
                print(educ)
        except KeyError:
            counters["educations"]["is_not_provided"] += 1

    pretty_print_dict(counters)

    return None


#check15_talent()

import datetime as dt
def sorts_hist_by_date(hist_list):

    f = lambda h: dt.datetime.strptime(h["since"], "%Y-%m-%d")
    sorted_hists = sorted(hist_list, key=f)
    return sorted_hists

def flatten_hist(hist):
    flattened = dict()
    for k,v in hist.items():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                flattened[f"{k}.{k2}"] = v2
        else:
            flattened[f"{k}"] = v
    return flattened

def get_what_changed(hist_1, hist_2):
    f_hist_1 = flatten_hist(hist_1)
    f_hist_2 = flatten_hist(hist_2)
    h1_keys = list(f_hist_1.keys())
    h2_keys = list(f_hist_2.keys())

    changes = list()
    for k,v in f_hist_1.items():
        if k == "since":
            continue # We skip this cause we are checking against this key
        if k not in h2_keys:
            changes.append(f"{k} key lost.")
        else:
            if f_hist_2[k] != v:
                changes.append(k)

    for k,v in f_hist_2.items():
        if k not in h1_keys:
            changes.append(f"{k} key added.")


    return changes

def check16_talent():

    talents = whoz["tale"]


    counters = {
        'history': {
            'is_empty': 0,
            'is_missing': 0,
            'appears': 0,
            'single_value': 0,
            'first_scopes': set(),
            'last_scopes': set(),
            'times_removed_is_last': 0,
            'times_removed_appears': 0,
            'times_changes_in_hist_lower_than_len': 0,
            'times_changes_in_hist_same_as_len': 0
        },
        'recruitment': {
            'is_empty': 0,
            'is_missing': 0,
            "appears": 0
        },
        'both': {
            're_empty_hist_filled': 0,
            'hist_empty_re_filled': 0,
            'both_empty': 0,
            'both_filled': 0,
        }
    }

    for tal in talents:
        conditions = {
            'rec_filled': False,
            'hist_filled': False,
        }
        try:
            rec = tal["recruitment"]
            counters["recruitment"]["appears"] += 1
            if len(rec) == 0:
                counters["recruitment"]["is_empty"] += 1
            else:
                conditions["rec_filled"] = True
        except KeyError:
            counters["recruitment"]["is_missing"] += 1

        try:
            hist = tal["history"]
            counters["history"]["appears"] += 1
            if len(hist) == 0:
                counters["history"]["is_empty"] += 1
            elif len(hist) == 1:
                counters["history"]["single_value"] += 1
            else:
                conditions["hist_filled"] = True

                n_changes = len(hist) - 1 # -1 cause there is always one less variation than the number of elements in a list.
                
                
                s_hist = sorts_hist_by_date(hist)
                prev_hist = s_hist[0]

                counters["history"]["first_scopes"].add(prev_hist["scope"])
                all_changes = list()
                for i, current_hist in enumerate(s_hist):
                    if current_hist["scope"] == "REMOVED":
                        counters["history"]["times_removed_appears"] += 1
                        if i== n_changes:
                            counters["history"]["times_removed_is_last"] += 1
                    
                    if i == 0:
                        continue

                    changes = get_what_changed(prev_hist, current_hist)

                    all_changes.append(changes)

                    prev_hist = current_hist

                counters["history"]["last_scopes"].add(current_hist["scope"])
                change_counter = 0
                for changes in all_changes:
                    if len(changes) > 0:
                        change_counter += 1

                if change_counter < n_changes:
                    counters["history"]["times_changes_in_hist_lower_than_len"] += 1
                else:
                    counters["history"]["times_changes_in_hist_same_as_len"] += 1


        except KeyError:
            counters["history"]["is_missing"] += 1

        if conditions["hist_filled"] and not conditions["rec_filled"]:
            counters["both"]["re_empty_hist_filled"] += 1

        if conditions["rec_filled"] and not conditions["hist_filled"]:
            counters["both"]["hist_empty_re_filled"] += 1

        if not conditions["hist_filled"] and not conditions["rec_filled"]:
            counters["both"]["both_empty"] += 1

        if conditions["hist_filled"] and conditions["rec_filled"]:
            counters["both"]["both_filled"] += 1


    print(len(talents))
    pretty_print_dict(counters)
    return None


check16_talent()







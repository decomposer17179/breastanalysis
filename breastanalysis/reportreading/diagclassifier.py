#-------------------------------------------------------------------------------
# Name:        diagclassifier
# Purpose:     Classify pathology reports according to diagnosis column
#              content.
#
# Author:      kpchang
#
# Created:     24/07/2016
# Copyright:   (c) kpchang 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from reportreading import reportreader
import pathlib

#First define macros for various organ systems.
CNS = 0
EYE = 1
EAR = 2
ORAL_CAVITY = 3
LIP = 4
NASOPHARYNX = 5
OROPHARYNX = 6
HYPOPHARYNX = 7
MAJORSALIVARY = 8
THYROID = 9
PARATHYROID = 10
LUNG_TRACHEA = 11
HEART = 12
STOMACH = 13
LIVER = 14
SPLEEN = 15
GALLBLADDER = 16
PANCREAS_PERIAMPULLA = 17
SMALLINTESTINE = 18
LARGEINTESTINE = 19
KIDNEY = 20
ADRENAL = 21
URETER = 22
URINARYBLADDER = 23
URETHRA = 24
TESTIS = 25
EPIDIDYMIS = 26
PROSTATE = 27
PENIS = 28
OVARY = 29
FALLOPIAN_TUBE = 30
UTERUS = 31
VAGINA = 32
SKIN = 33
SOFTTISSUE_BONE = 34
LYMPHNODE = 35
PERITONEUM = 36
PLEURA = 37
LARYNX = 38
ESOPHAGUS = 39
FAILTOCLASSIFY = 40
BREAST = 41
THYMUS = 42
PLACENTA = 43
APPENDIX = 44
ANUS = 45
FOREIGNBODY = 46
PARANASALSINUS = 47
PHARYNX = 48

#Then define macros for diagnosis classification
CARCINOMA = 0
SARCOMA = 1
LYMPHOMA = 2
MELANOMA = 3
NET = 4
GIST = 5
GLIOMA = 6
BLASTOMA = 7
GERMCELL = 8
THYMOMA = 9
OTHERS = 10

#Names for the organ codes:
organ_code_name = ["Central Nervous System", "Eyes", "Ear", "Oral Cavity", "Lip",
            "Nose and Nasopharynx", "Oropharynx", "Hypopharynx", "Major Salivary Gland",
            "Thyroid gland", "Parathyroid gland", "Lung, Bronchus and Trachea",
            "Heart", "Stomach", "Liver", "Spleen", "Gallbladder",
            "Pancreas and peri-ampulla region", "Small Intestine",
            "Large intestine", "Kidney", "Adrenal gland", "Ureter",
            "Urinary Bladder", "Urethra", "Testis", "Epididymis",
            "Prostate, Seminal Vesicle and Vas deferens", "Penis", "Ovary",
            "Fallopian Tube", "Uterus", "Vagina", "Skin", "Soft tissue and bone",
            "Lymph nodes", "Peritoneum", "Pleura", "Larynx", "Esophagus",
            "Program Failed to Classify", "Breast", "Mediastinum and Thymus", "Placenta and umbilical cord",
            "Appendix", "Anus", "Foreign body", "Paranasal sinus", "Pharynx, unspecified"]
#Special organ type:
special_gi = [ESOPHAGUS, STOMACH, SMALLINTESTINE, LARGEINTESTINE, PANCREAS_PERIAMPULLA,
            GALLBLADDER]
special_chest = [LUNG_TRACHEA, PLEURA, THYMUS]
special_germ = [OVARY, TESTIS]

#tokens that categorize organ into organ codes
organ_token = {"brain":CNS, "cerebrum":CNS, "cerebellum":CNS, "pituitary":CNS,
                "pineal":CNS, "eye": EYE, "ear, middle": EAR, "oral": ORAL_CAVITY,
                "oral cavity": ORAL_CAVITY, "gingiva": ORAL_CAVITY, "lip": LIP,
                "tongue": ORAL_CAVITY, "buccal": ORAL_CAVITY, "gum": ORAL_CAVITY,
                "nose": NASOPHARYNX, "nasal": NASOPHARYNX, "nasopharynx": NASOPHARYNX,
                "oropharynx": OROPHARYNX, "tonsil": OROPHARYNX, "hypopharynx": HYPOPHARYNX,
                "soft palate": OROPHARYNX, "hard palate": ORAL_CAVITY,
                "larynx": LARYNX, "vocal": LARYNX, "salivary": MAJORSALIVARY,
                "parotid": MAJORSALIVARY, "submandibular": MAJORSALIVARY, "sublingual": MAJORSALIVARY,
                "thyroid": THYROID, "parathyroid": PARATHYROID, "lung": LUNG_TRACHEA,
                "bronchus": LUNG_TRACHEA, "trachea": LUNG_TRACHEA, "heart": HEART,
                "valve": HEART, "esophag": ESOPHAGUS, "stomach": STOMACH,
                "cardia": STOMACH, "liver": LIVER, "spleen": SPLEEN,
                "gallbladder": GALLBLADDER, "pancreas": PANCREAS_PERIAMPULLA,
                "ampulla": PANCREAS_PERIAMPULLA, "common bile duct": PANCREAS_PERIAMPULLA,
                "small intestine": SMALLINTESTINE, "duodenum": SMALLINTESTINE,
                "jejunum": SMALLINTESTINE, "ileum": SMALLINTESTINE,
                "large intestine": LARGEINTESTINE, "colon": LARGEINTESTINE,
                "rectum": LARGEINTESTINE, "kidney": KIDNEY, "adrenal": ADRENAL,
                "ureter": URETER, "urinary": URINARYBLADDER, "urethra": URETHRA,
                "testis": TESTIS, "epididymis": EPIDIDYMIS, "prostate": PROSTATE,
                "seminal": PROSTATE, "vas": PROSTATE, "penis": PENIS,
                "ovary": OVARY, "fallopian": FALLOPIAN_TUBE, "uterus": UTERUS,
                "cervix": UTERUS, "endometrium": UTERUS, "endocervix": UTERUS,
                "vagina": VAGINA, "skin": SKIN, "scalp": SKIN, "scrotum": SKIN,
                "soft tissue":SOFTTISSUE_BONE, "bone": SOFTTISSUE_BONE,
                "vessel": SOFTTISSUE_BONE, "sac": SOFTTISSUE_BONE,
                "muscle": SOFTTISSUE_BONE, "lymph node": LYMPHNODE,
                "peritoneum": PERITONEUM, "serosa": PERITONEUM,
                "pleura": PLEURA, "breast": BREAST, "thymus":THYMUS, "mediastinum": THYMUS,
                "placenta": PLACENTA, "umbilical cord": PLACENTA, "small": SMALLINTESTINE,
                "appendix": APPENDIX, "cecum": LARGEINTESTINE, "joint": SOFTTISSUE_BONE,
                "anus": ANUS, "disc": SOFTTISSUE_BONE, "foreign": FOREIGNBODY, "sinus": PARANASALSINUS,
                "vein": SOFTTISSUE_BONE, "femoral head": SOFTTISSUE_BONE, "foot": SOFTTISSUE_BONE,
                "toe": SOFTTISSUE_BONE, "hand": SOFTTISSUE_BONE, "vulva": SKIN,
                "aorta": SOFTTISSUE_BONE, "sigmoid": LARGEINTESTINE, "prepuce": SKIN,
                "neck": SOFTTISSUE_BONE, "uterine": UTERUS, "cranial": CNS,
                "hepatic duct": PANCREAS_PERIAMPULLA, "ear,": EAR, "e-tube": EAR,
                "anal": ANUS, "spine": SOFTTISSUE_BONE, "extremity": SOFTTISSUE_BONE,
                "spinal cord": CNS, "tendon": SOFTTISSUE_BONE, "parathyoid": PARATHYROID,
                "bucca": ORAL_CAVITY, "fallpian tube": FALLOPIAN_TUBE, "synovi": SOFTTISSUE_BONE,
                "bursa": SOFTTISSUE_BONE, "pericardium": HEART, "artery": SOFTTISSUE_BONE,
                "peritoneal": PERITONEUM, "spine": SOFTTISSUE_BONE, "omentum": PERITONEUM,
                "sentinel node": LYMPHNODE, "meninx": CNS, "meninge": CNS,
                "cartilage": SOFTTISSUE_BONE, "ovarian": OVARY, "auricle": EAR,
                "eardrum": EAR, "pinna": EAR, "nostril": NASOPHARYNX, "dura": CNS,
                "orbit": EYE, "thryoid": THYROID, "nipple": SKIN, "finger": SOFTTISSUE_BONE,
                "auricle": EAR, "mesentery": PERITONEUM, "mediastinal": THYMUS,
                 "face": SKIN, "ligament": SOFTTISSUE_BONE, "ligment": SOFTTISSUE_BONE,
                 "middle ear": EAR, "mastoid": EAR, "epiglottis": HYPOPHARYNX,
                 "renal pelvis":KIDNEY, "thumb": SOFTTISSUE_BONE, "cavernosum": PENIS,
                 "conju": EYE, "diaphragm": SOFTTISSUE_BONE, "vertebra": SOFTTISSUE_BONE,
                 "arm": SOFTTISSUE_BONE, "palm": SOFTTISSUE_BONE}

#Names for the diagnosis codes:
diag_code_name = ["Carcinoma", "Sarcoma", "Lymphoma", "Melanoma", "Neuroendocrine tumor",
                "Gastrointestinal stromal tumor", "Gliomas", "Blastomas",
                "Germ cell tumor", "Thymoma", "Others"]
#token for diagnosis codes
diag_token_majormalignant = {"carcinoma": CARCINOMA, "malignant pilar": CARCINOMA,
            "sarcoma": SARCOMA, "malignant solitary fibrous": SARCOMA,
            "malignant peripheral nerve": SARCOMA, "mpnst": SARCOMA,
            "malignant granular": SARCOMA, "lymphoma": LYMPHOMA,
            "maltoma": LYMPHOMA, "melanoma": MELANOMA, "leukemia": LYMPHOMA}

diag_token_gi = {"neuroendocrine": NET, "carcinoid": NET, "insulinoma": NET,
                        "glucagon": NET, "gastrointestinal stromal tumor": GIST,
                        "gist": GIST, "blastoma": BLASTOMA}

diag_token_cns = {"glioma": GLIOMA, "astrocytoma": GLIOMA, "glioblastoma": GLIOMA,
                "gliosarconma": GLIOMA, "pineoblastoma": BLASTOMA, "pnet": BLASTOMA,
                "medulloblastoma": BLASTOMA, "germinoma": GERMCELL,
                "germ cell": GERMCELL, "yolk sac": GERMCELL, "ependymoma": GLIOMA}

diag_token_chest = {"carcinoid": NET, "blastoma": BLASTOMA, "thymoma": THYMOMA}

diag_token_germ = {"dysgerminoma": GERMCELL, "seminoma": GERMCELL,
                "germ cell": GERMCELL, "yolk sac": GERMCELL, "chorio": GERMCELL}

#Wrapper for macros in reportreader
AGE_COLUMN = "age column"
DIAG_COLUMN = "diagnosis column"
DESC_COLUMN = "description column"
REF_COLOMN = "reference column"
ORGANNAME = "organ name"
POSITION = "position"
PROCEDURE = "procedure"
DIAGNOSIS = "diagnosis"
LEFT = "left"
RIGHT = "right"
POSITIVE = "positive"
NEGATIVE = "negative"
PATHOLOGYNO = "pathology number"

def search_organ_token(some_str):
    """
        Search for organ token in some given strings.
    """
    some_str = some_str.lower()
    for item in organ_token.keys():
        if item in some_str:
            return organ_token[item]
    return FAILTOCLASSIFY

def search_diag_token(some_str, organcode):
    """
        Search for diagnosis token in some given strings.
        First search for major malignancy,
        then some special tumor in CNS, GI, CHEST, and Germ cells
    """
    for item in diag_token_majormalignant.keys():
        if item in some_str:
            return diag_token_majormalignant[item]
    if organcode in special_gi:
        for item in diag_token_gi.keys():
            if item in some_str:
                return diag_token_gi[item]
        return OTHERS
    elif organcode in special_chest:
        for item in diag_token_chest.keys():
            if item in some_str:
                return diag_token_chest[item]
        return OTHERS
    elif organcode in special_germ:
        for item in diag_token_germ.keys():
            if item in some_str:
                return diag_token_germ[item]
        return OTHERS
    elif organcode == CNS:
        for item in diag_token_cns.keys():
            if item in some_str:
                return diag_token_cns[item]
        return OTHERS
    else:
        return OTHERS

def diag_classify(diags):
    """
        Feed one line of diagnosis, and give organ token and diagnosis token.
    """
    diagdict = reportreader.take_diag_data(diags)
    organname = diagdict[reportreader.ORGANNAME] + ",".join(diagdict[reportreader.POSITION])
    diagstr = " ".join(diagdict[reportreader.DIAGNOSIS])
    organcode = search_organ_token(organname)
    #print(organcode)
    diagcode = search_diag_token(diagstr, organcode)
    #print(diagcode)
    return (organcode, diagcode, organ_code_name[organcode],
            diag_code_name[diagcode])

def first_diag_classify(li):
    """
        Get a list of diagnoses, find first diagnosis, and classify
    """
    i = 0
    # only recognize first diagnosis column when contain_procedure
    while True:
        words = li[i].split(",")
        if i >= (len(li)-1):
            break
        elif "section report" in li[i]:
            #print(li[i])
            i = i+1
            continue
        elif reportreader.contain_procedure(words):
            #print(True)
            break
        else:
            #print(False)
            i = i+1
            continue
    #print(li[i])
    return diag_classify(li[i])

#To avoid cyclic import, need a wrapper for reportreader.read_report here
def wrap_read_report(fname):
    return reportreader.read_report(fname)


def main():
    print(organ_code_name[CNS], organ_code_name[THYMUS], organ_code_name[FAILTOCLASSIFY])
    print(diag_code_name[GLIOMA], diag_code_name[GERMCELL], diag_code_name[CARCINOMA])
    folderpath = pathlib.Path("testfile")
    filelist = list(folderpath.glob("*.*"))
    reports = []
    for item in filelist:
        itemname = str(item)
        try:
            reports.append(wrap_read_report(itemname))
        except ValueError:
            pass
    for report in reports:
        crudediag = report[DIAG_COLUMN]
        print(first_diag_classify(crudediag))

if __name__ == '__main__':
    main()

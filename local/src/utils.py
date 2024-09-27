import datetime


def ts_to_ymdh(ts):
    if not ts:
        ts = datetime.datetime.now(datetime.UTC)
    ymdh = {}
    ymdh["y"] = ts.strftime("%Y")
    ymdh["m"] = ts.strftime("%m")
    ymdh["d"] = ts.strftime("%d")
    ymdh["h"] = ts.strftime("%H")

    return ymdh


import os


def get_file_paths_matching(directory, ext=""):
    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(ext):
                file_path = os.path.join(root, file)
                file_paths.append(file_path)
    return file_paths

import os


def fast_scandir(dir, ext):  # dir: str, ext: list
    subfolders, files = [], []

    for f in os.scandir(dir):
        if f.is_dir():
            subfolders.append(f.path)
        if f.is_file():
            if os.path.splitext(f.name)[1].lower() in ext:
                files.append(f.path)
    for dir in list(subfolders):
        sf, f = fast_scandir(dir, ext)
        subfolders.extend(sf)
        files.extend(f)
    return subfolders, files


# return type,exchange,code, date , full_path
def meta_from_quotefile_path(fn):
    h, t = os.path.split(fn)
    if t[:len('SnapShot')] == 'SnapShot':
        exchange = t[len("SnapShot"):len("SnapShot") + 2]
        code = t[len("SnapShot") + 2:-4]
    if t[:len('Transaction')] == 'Transaction':
        exchange = t[len("Transaction"):len("Transaction") + 2]
        code = t[len("Transaction") + 2:-4]
    if t[:len('Order')] == 'Order':
        exchange = t[len("Order"):len("Order") + 2]
        code = t[len("Order") + 2:-4]
    h, t = os.path.split(h)
    qtype = t
    h, t = os.path.split(h)
    qdate = t
    return qtype, exchange, code, qdate, fn


def enum_quotefiles_with_meta(dir, ext):
    subfolders, files = fast_scandir(dir, ext)
    all_files = []

    for f in files:
        fn_full_path = os.path.abspath(f)
        type, exchange, code, date, full_path = meta_from_quotefile_path(fn_full_path)
        if full_path is not None:
            all_files.append([type, exchange, code, date, full_path])

    return all_files


# return type,exchange,code, date , full_path
def meta_from_snapshotfile_path(fn):
    p, t = os.path.split(fn)
    p, date = os.path.split(p)
    res = t.split('.')  # sz.300081.sol.csv
    if len(res) != 4:
        return None, None, None, None, None

    return res[2], res[0], res[1], date, fn


def enum_snapshotfiles_with_meta(dir, ext):
    subfolders, files = fast_scandir(dir, ext)
    all_files = []

    for f in files:
        fn_full_path = os.path.abspath(f)
        type, exchange, code, date, full_path = meta_from_snapshotfile_path(fn_full_path)
        if full_path is not None:
            all_files.append([type, exchange, code, date, full_path])

    return all_files

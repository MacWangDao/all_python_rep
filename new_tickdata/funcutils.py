def full_merge_dict(d1, d2):
    for key, value in d1.items():
        if key in d2:
            if type(value) is dict:
                full_merge_dict(d1[key], d2[key])
            else:
                if type(value) in (int, float, str):
                    d1[key] = [value]
                if type(d2[key]) is list:
                    d1[key].extend(d2[key])
                else:
                    d1[key].append(d2[key])
    for key, value in d2.items():
        if key not in d1:
            d1[key] = value


def pad_or_truncate(some_list, target_len, tail=True):
    return some_list[:target_len] + [0] * (target_len - len(some_list)) if tail else \
        [0] * (target_len - len(some_list)) + some_list[:target_len]

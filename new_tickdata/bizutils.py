import os

#sh.600000
def price_precision(instrument):
    si = instrument.split('.')
    if si[0].startswith("sz") and si[1].startswith("1"):
        return 3
    if si[0].startswith("sh") and si[1].startswith("5"):
        return 3
    return 2

def price_precision_fmt(instrument):
    dec = price_precision(instrument)
    return '''{:.%df}'''%(dec)









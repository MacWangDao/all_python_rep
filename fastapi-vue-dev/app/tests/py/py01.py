import pypinyin

from pypinyin import pinyin
from pypinyin import lazy_pinyin, Style

print(pinyin("浙商银行", style=Style.FIRST_LETTER))
print("".join(lazy_pinyin("浙商银行", style=Style.FIRST_LETTER)))

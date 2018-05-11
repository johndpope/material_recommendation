# encoding=utf8

import os
import re
from chunyu.utils.general.encoding_utils import ensure_unicode
from global_config import get_root_path
from general_utils.file_utils import load_simple_lines

STOP_WORD_PATH = os.path.join(get_root_path(), 'general_utils/general_data_dir/stop_words.txt')
E_STOP_WORD_PATH = os.path.join(get_root_path(), 'general_utils/general_data_dir/e_stop_words.txt')
_stopword_handler = None


class Stopwords(object):
    data = load_simple_lines(STOP_WORD_PATH)
    e_data = load_simple_lines(E_STOP_WORD_PATH)

    @classmethod
    def is_stop_word(cls, word):
        word = ensure_unicode(word)
        return word in cls.data

    @classmethod
    def is_e_stop_word(cls, word):
        word = ensure_unicode(word)
        return word in cls.e_data


def get_stop_word_handler():
    global _stopword_handler
    if not _stopword_handler:
        _stopword_handler = Stopwords()
    return _stopword_handler


def filterHTML(htmlstr):
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\s*?/?>')  # 处理换行
    re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    # s = replaceCharEntity(s)  # 替换实体

    return s

    ##替换常用HTML字符实体.
    # 使用正常的字符替换HTML中特殊的字符实体.
    # 你可以添加新的实体字符到CHAR_ENTITIES中,处理更多HTML字符实体.
    # @param htmlstr HTML字符串.


def repalce(s, re_exp, repl_string):
    return re_exp.sub(repl_string, s)


def convert2gbk(a_list):
    another_list = []
    for x in a_list:
        if isinstance(x, basestring):
            another_list.append(x.encode('gbk', 'ignore'))
        else:
            another_list.append(str(x).encode('gbk', 'ignore'))
    return another_list


def qa_ask_info(text):
    sex = ''
    age = ''
    text = ensure_unicode(text).strip()
    ask_tail_pattern = re.compile(u"（.{1,8}）$")
    sex_pattern = re.compile(u"[男|女]")

    tail = ask_tail_pattern.search(text)

    if tail:
        text = ask_tail_pattern.sub("", text)
        tail = tail.group(0)
        sex0 = sex_pattern.search(tail)
        if sex0:
            sex = sex0.group(0)
        age = sex_pattern.sub(u"", tail.replace(u"（", u"").replace(u"）", "")).replace(u"，", u"").strip()
    return text, sex, age


def is_chinese(uchar, but=u''):
    """判断一个unicode是否是汉字"""
    if uchar == but or uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False


def is_chinese_all(ustring, but=u''):
    """判断一个unicode串是否是汉字串"""
    for uchar in ustring:
        if not is_chinese(uchar, but):
            return False
    return True


def is_number(uchar, but=u''):
    """判断一个unicode是否是数字"""
    if uchar == but or uchar >= u'\u0030' and uchar <= u'\u0039':
        return True
    else:
        return False


def is_number_all(ustring, but=u''):
    """判断一个unicode串是否是数字串"""
    for uchar in ustring:
        if not is_number(uchar, but):
            return False
    return True


def is_alphabet(uchar, but=u''):
    """判断一个unicode是否是英文字母"""
    if uchar == but or (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
        return True
    else:
        return False


def is_alphabet_all(ustring, but=u''):
    """判断一个unicode串是否是英文字母串"""
    for uchar in ustring:
        if not is_alphabet(uchar, but):
            return False
    return True


def is_alphanum(uchar, but=u''):
    """判断一个unicode是否是英文字母或数字"""
    if is_number(uchar, but) or is_alphabet(uchar, but):
        return True
    else:
        return False


def is_alphanum_or_number_single(ustr):
    # 判断str是不是长度为1的纯字母或纯数字
    if len(ustr) != 1:
        return False
    return is_alphanum(ustr)


def is_alpha_or_num_all(ustring, but=u''):
    """判断一个unicode串是否是英文字母或数字串"""
    for uchar in ustring:
        if not is_alphanum(uchar, but):
            return False
    return True


def is_alpha_and_num_all(ustring, but=u''):
    """判断一个unicode串是否是英文字母及数字串"""
    alphabet = 0
    number = 0
    for uchar in ustring:
        if is_alphabet(uchar, but):
            alphabet += 1
        elif is_number(uchar, but):
            number += 1
        else:
            return False
    if alphabet > 0 and number > 0:
        return True
    else:
        return False


def is_other(uchar, but=u''):
    """判断是否非汉字、数字和英文字符"""
    if not (is_chinese(uchar) or is_number(uchar, but) or is_alphabet(uchar, but)):
        return True
    else:
        return False


def what(uchar):
    if len(uchar) > 1:
        print 'not a char'
        return
    if is_chinese(uchar):
        return 'c'
    if is_alphabet(uchar):
        return 'a'
    if is_number(uchar):
        return 'n'
    return 'o'


def remove_other(uword, join=u''):
    out = u''
    for x in uword:
        if is_other(x) is False:
            out = out + x
        else:
            out += join
    return out


def is_other_all(ustring, but=u''):
    """判断是否非汉字、数字和英文字符串"""
    for uchar in ustring:
        if not is_other(uchar, but):
            return False
    return True


def exist_chinese(ustring):
    for uchar in ustring:
        if is_chinese(uchar):
            return True
    return False


def exist_number(ustring):
    for uchar in ustring:
        if is_number(uchar):
            return True
    return False


def exist_alphabet(ustring):
    for uchar in ustring:
        if is_alphabet(uchar):
            return True
    return False


def exist_other(ustring, but=u''):
    for uchar in ustring:
        if is_other(uchar, but):
            return True
    return False


def B2Q(uchar):
    """半角转全角"""
    inside_code = ord(uchar)
    if inside_code < 0x0020 or inside_code > 0x7e:  # 不是半角字符就返回原来的字符
        return uchar
    if inside_code == 0x0020:  # 除了空格其他的全角半角的公式为:半角=全角-0xfee0
        inside_code = 0x3000
    else:
        inside_code += 0xfee0
    return unichr(inside_code)


def Q2B(uchar):
    """全角转半角"""
    if uchar == u'’' or uchar == u'‘':  # 很多全角字符不能转：【】￥。×
        return u'\''  # 规范x号
    inside_code = ord(uchar)
    if inside_code == 0x3000:
        inside_code = 0x0020
    else:
        inside_code -= 0xfee0
    if inside_code < 0x0020 or inside_code > 0x7e:  # 转完之后不是半角字符返回原来的字符
        return uchar
    return unichr(inside_code)


def stringQ2B(ustring):
    """把字符串全角转半角"""
    return "".join([Q2B(uchar) for uchar in ustring])


def uniform(ustring):
    """格式化字符串，完成全角转半角，大写转小写的工作"""
    return stringQ2B(ustring).lower()


def string2List(ustring):
    """将ustring按照中文，字母，数字分开"""
    retList = []
    utmp = []
    for uchar in ustring:
        if is_other(uchar):
            if len(utmp) == 0:
                continue
            else:
                retList.append("".join(utmp))
                utmp = []
        else:
            utmp.append(uchar)
    if len(utmp) != 0:
        retList.append("".join(utmp))
    return retList


BABY_WORDS = [
    u"宝宝",
    u"小孩",
    u"孩子",
    u"婴",
    u"娃",
    u"儿科",
    u"小儿",
    u"新生儿",
    u"幼儿",
    u"小宝",
    u"儿子",
    u"女儿",
    u"小朋友",
    u"男孩",
    u"女孩",
    u"女宝",
    u"男宝",
    u"双胞胎",
    u"闺女",
    u"娃娃",
    u"儿童",

]

BABY_SEARCH = [
    re.compile(ur"[0-9一二两三四五六七八九十]{1,3}(个月)"),
]


def is_baby_text(text):
    '''
    用规则判断是不是宝宝文本
    :param text:
    :return:
    '''
    text = ensure_unicode(text)
    for x in BABY_WORDS:
        if x in text:
            return True
    for p in BABY_SEARCH:
        if p.search(text):
            return True
    return False




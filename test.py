def verify_args(string, char_seq, pre_char):
    if not all([string, char_seq, pre_char]):
        raise ValueError(f"missing mandatory args: {string=}, {char_seq=}, {pre_char=}")


def do_search_and_preplace_foreach(string, char_seq, pre_char):
    verify_args(string, char_seq, pre_char)
    accum = ""
    for pos, val in enumerate(string):
        if pos + len(char_seq) <= len(string):
            if string[pos : pos + len(char_seq)] == char_seq:
                accum += pre_char
        accum += val
    return accum


# via stringlib
def do_search_and_preplace_strlib(string, char_seq, pre_char):
    verify_args(string, char_seq, pre_char)
    return f"{pre_char}{char_seq}".join(string.split(char_seq))


if __name__ == "__main__":
    string = "abcalphacdealphaxalph"
    char_seq = "alpha"
    pre_char = "_"
    result = do_search_and_preplace_foreach(
        string=string, char_seq=char_seq, pre_char=pre_char
    )
    print(result)
    assert result == "abc_alphacde_alphaxalph"
    result = do_search_and_preplace_strlib(
        string=string, char_seq=char_seq, pre_char=pre_char
    )
    print(result)
    assert result == "abc_alphacde_alphaxalph"

import string


def preprocessing(st):
    for i in st:
        if i in string.punctuation:
            st = st.replace(i, '')
    st = st.upper()
    return st

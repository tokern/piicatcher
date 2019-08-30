from spacy.lang.en import English


class Tokenizer:
    def __init__(self):
        nlp = English()
        # Create a Tokenizer with the default settings for English
        # including punctuation rules and exceptions
        self._tokenizer = nlp.Defaults.create_tokenizer(nlp)

    def tokenize(self, data):
        return self._tokenizer(data)

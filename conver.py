import srsly
import spacy
from spacy.tokens import DocBin

nlp = spacy.blank("en")

def convert(jsonl_file, spacy_file):
    doc_bin = DocBin()
    for line in srsly.read_jsonl(jsonl_file):
        text = line["text"]
        ents = line["entities"]
        doc = nlp.make_doc(text)
        spans = []
        for start, end, label in ents:
            span = doc.char_span(start, end, label=label)
            if span:
                spans.append(span)
        doc.ents = spans
        doc_bin.add(doc)
    doc_bin.to_disk(spacy_file)
    print("Saved:", spacy_file)

convert("train.jsonl", "train.spacy")
convert("dev.jsonl", "dev.spacy")
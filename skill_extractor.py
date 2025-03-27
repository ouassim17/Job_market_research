import spacy

nlp = spacy.load("en_core_web_sm")

# Define your custom dictionary of words with labels
ruler = nlp.add_pipe("entity_ruler", before="ner")

patterns = [
    {"label": "SKILL", "pattern": "Python"},
    {"label": "SKILL", "pattern": "TensorFlow"},
    {"label": "SKILL", "pattern": "Keras"},
    {"label": "SKILL", "pattern": "Machine Learning"},
    {"label": "SKILL", "pattern": "sparkSQL"}
]

# Add patterns to the ruler
ruler.add_patterns(patterns) # type: ignore



# Processing the text
with open("test_description.txt", "r") as text_file:
    text = text_file.read()

doc = nlp(text)

# Print Named Entities
for ent in doc.ents:
    print(ent.text, "->", ent.label_)
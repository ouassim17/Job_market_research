import subprocess
import spacy


try:
    nlp = spacy.load("en_core_web_sm")
    print("Model already installed.")
except OSError:
    print("Downloading model...")
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")  
    print("Download complete!")


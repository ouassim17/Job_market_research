from celery_app import app
from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi


@app.task(
    name="rekrute",
)
def rekrute_task():
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e} ")


@app.task(
    name="bayt",
)
def bayt_task():
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e} ")


@app.task(
    name="Marocannonce",
)
def marocann_task():
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi marocann: {e} ")


@app.task(
    name="emploi",
)
def emploi_task():
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e} ")

from selenium_init import *
data=[{
      "job_url": "www.example.com",
      "title": "program tester",
      
      "salaire": 0,
      "domaine": "IT",
      
      "publication_date": "today"
    },{
      "job_url": "www.example22.com",
      "title": "food critic",
      
      "salaire": 6000,
      "domaine": "gastornomy",
      
      "publication_date": "10 days ago"
    }]
save_json(data, "offres_emploi_rekrute.json")
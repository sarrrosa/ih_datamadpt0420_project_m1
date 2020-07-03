# Where are data analysts working from? A country analysis
![Image](https://baunsreit.es/wp-content/uploads/2020/07/data_analysts.png)

###Motivation :blush:
*This is my first project within the Ironhack Analytics Bootcamp (Part time version) in Madrid.*

### Overview :earth_africa:
The result of this project is to analyse how **data-related analysts** are working **depending on their location (countryside or urban)** and analyse these patterns according to the country they're living in. **You will also be able to download a CSV with the requested table** or even **analyse the results per country**, too.

---

### Data sources :scroll:
 - Database with the job codes and the demographic information
    - Provided by [Ironhack](http://www.potacho.com/files/ironhack/raw_data_project_m1.db) formatted as a `.db` file.
 - API access to Swagger data created by [Work Data Initiative](http://api.dataatwork.org/v1/jobs/autocomplete?contains=data
)
 - Scraping the country codes from [Eurostat](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes)

### Requirements :arrow_forward:

You will need to install and import or have already installed and imported the following Python libraries in order to be able to analyse the data:
- Data analysis:
    - Pandas
    - Functools (Reduce)
    - Numpy
    - BeautifulSoup
    - Re
    - Sqlalchemy
    - Requests
- Data visualisation
    - Seaborn
    - Matplotlib

### Folder structure :bookmark_tabs:

```
└── project
    ├── __trash__    
    └── data
        ├── raw
        ├── processed
        └── results
    ├── p_acquisition
        ├── m_acquisition.py
    ├── p_analysis
        ├── m_analysis.py
    ├── p_reporting
        ├── m_reporting.py
    ├── p_wrangling
        ├── m_wrangling.py
    ├── .env.txt
    ├── .gitignore
    ├── main_script.py
    ├── README.md
    ├── requirements.txt
```
### Country analysis :crystal_ball:
The following outputs will be available for you to download as a `.csv` 
- Download the csv table with all the countries analysed and the representative percentage
- Download the csv table with only one country analysed

In order to analyse the country of your choice, you can choose from the following table, scraped from [Eurostat](https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes)
- Austria
- Belgium
- Bulgaria
- Croatia
- Cyprus
- Czechia
- Denmark
- Estonia
- Finland
- France
- Germany
- Greece
- Hungary
- Ireland
- Italy
- Latvia
- Lithuania
- Luxembourg
- Malta
- Netherlands
- Poland
- Portugal
- Romania
- Slovakia
- Slovenia
- Spain
- Sweden
- United Kingdom

### How to use :electric_plug:
Run the main_scrip.py in your terminal to check the results. You can either run the script indicating the path selected or you can either select a country of your choice and get the `.csv`  file with the country input.

### Country analysis :tada:
An example of the input that you will be able to retrieve is the following:

![Image](https://baunsreit.es/wp-content/uploads/2020/07/spain.png)

### Next steps - WIP :telescope:
- Build a function that will retrieve visualisations for the data analysed directly into the results folder
- Be able to send the output automatised by email

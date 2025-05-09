# 🏏 PSL Cricket ETL Pipeline – Web Scraping, Cleaning & Loading

An end-to-end automated data pipeline that extracts, transforms, and loads over **8,000+ player-level PSL match records (2016–2024)** from ESPNcricinfo using Python, Selenium, and AWS RDS. This project demonstrates real-world web scraping, concurrency, data engineering, and cloud integration.


---
## 📁 Project Structure

```text
├── Data/                   # Cleaned CSV outputs (batting, bowling, player data)
├── Html/                   # Raw HTML files extracted from ESPNcricinfo
├── .gitignore              # Git ignore rules
├── app.py                  # Main entry script for running the pipeline
├── Data_Extraction.py      # Web scraping logic using Selenium + BeautifulSoup
├── Data_Transformation.py  # Data cleaning and structuring using Pandas
├── Load_Data.py            # Loads data into AWS RDS MySQL using SQLAlchemy
├── requirements.txt        # Python dependencies
```
---
## 🛠️ Technologies Used
- Python 3.x  
- Selenium  
- BeautifulSoup4  
- Pandas  
- SQLAlchemy  
- AWS RDS (Aurora MySQL)  
- ThreadPoolExecutor  
- Jupyter Notebook  

---

## 🧪 Pipeline Overview

### 1. Extraction
- Uses Selenium with Microsoft Edge to open dynamic PSL match pages.  
- Applies `ThreadPoolExecutor` to parallelize match scraping.  
- BeautifulSoup parses HTML for batting, bowling, and player stats.  

### 2. Transformation
- Cleans inconsistent formats and edge cases using Pandas.  
- Stores intermediate results as structured CSV files in `Data/`.  

### 3. Loading
- Connects securely to an AWS RDS MySQL instance via SQLAlchemy.  
- Pushes final tables (batting, bowling, players) into the cloud.  

---

## ✅ Key Achievements

| Metric            | Result         | Impact                          |
|-------------------|----------------|---------------------------------|
| Collection Speed  | < 15 minutes   | 75% faster than sequential mode |
| Data Validity     | 99% valid      | Reliable analytics-ready dataset|
| Storage           | AWS RDS        | Scalable, cloud-based storage   |

---

## 📊 Applications
- EDA and visualization of PSL trends  
- Fantasy league player prediction  
- ML models for player performance  
- Cricket analytics blogs and dashboards  

---

## 📦 Setup Instructions

### 1. Clone the Repo
```bash
git clone https://github.com/alysahab/Web-Scraping-PSL-Data.git
cd Web-Scraping-PSL-Data
```
### 2. Create a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```
### 3. Install Dependencies
```bash
pip install -r requirements.txt
```
### 4. Run the Pipeline
```bash
python app.py
```

> 📊 [Explore the Dataset on Kaggle](https://www.kaggle.com/datasets/alysahab/complete-psl-data-2016-2024)  

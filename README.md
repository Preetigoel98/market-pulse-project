# Market Pulse: Automated Data Engineering Pipeline

An end-to-end Data Engineering project demonstrating the **Medallion Architecture** (Bronze, Silver, Gold) to analyze job market trends.

## 🏗️ Architecture
- **Bronze Layer**: Ingestion of raw job data into PostgreSQL.
- **Silver Layer**: Automated cleaning and standardization using Python/Pandas.
- **Gold Layer**: Analytical modeling (Skill Explosion) for salary insights.

## 🛠️ Tech Stack
- **Language**: Python 3.x
- **Libraries**: Pandas, SQLAlchemy, Psycopg2, Python-dotenv
- **Database**: PostgreSQL
- **Security**: Environment variables for credential management

## 🚀 How to Run
1. Clone the repository.
2. Create a `.env` file with your database credentials.
3. Install dependencies: `pip install -r requirements.txt`
4. Run the pipeline: `python scripts/bronze_to_silver.py`

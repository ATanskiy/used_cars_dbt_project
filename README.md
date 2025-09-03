# Used Cars DBT Project

**Used Cars DBT Project** is a data engineering pipeline that leverages DBT (Data Build Tool) to process, transform,
 and analyze used-car datasets. It orchestrates daily data ingestion, transformation, and analytics-ready modeling 
 using technologies like dbt, Docker, and Dagster.

---

## 📂 Project Structure

- **configs/** – Stores configuration files and settings for different environments.  
- **dagster_project/** – Contains Dagster workflows for orchestrating data pipelines.  
- **daily_data_loader/** – Scripts to ingest or load daily used car data.  
- **db_utils/** – Utility functions shared across DBT models and scripts.  
- **dbt/** – Core DBT project directory with models, snapshots, and transformations.  
- **dockerfiles/** – Docker configuration files to containerize pipeline components.  
- **functions/** – Shared function definitions and helper logic.  
- **kaggle_loader/** – Scripts to fetch data from Kaggle.  
- **scripts/** – Miscellaneous automation scripts for project workflows.  
- `.gitignore` – Defines files ignored by Git.  
- `docker-compose.yml` – Defines services and containers for local development.  
- `pyproject.toml` – Python dependencies and project metadata.  
- `requirements.txt` – Lists project Python dependencies.  

---

## 🚀 Getting Started

### Prerequisites
- **Docker & Docker Compose**  
- **Python** (3.8+)  
- **DBT** (installed locally or via Docker)  

### Setup and Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/ATanskiy/used_cars_dbt_project.git
   cd used_cars_dbt_project
   ```

2. **Start services using Docker Compose**
   ```bash
   docker-compose up -d
   ```
   This spins up required services like your database, dbt container, and orchestration engine.

3. **Install dependencies** (if running outside Docker)
   ```bash
   pip install -r requirements.txt
   ```

4. **Run DBT transformations**
   ```bash
   cd dbt
   dbt deps         # Install DBT dependencies
   dbt run          # Execute transformation models
   dbt test         # Run tests and validations
   dbt docs generate
   dbt docs serve   # View documentation locally
   ```

5. **Run Daily Data Loader & Orchestration**
   - Use scripts in **daily_data_loader/** to simulate or ingest daily data.  
   - Launch Dagster workflows from **dagster_project/** for end-to-end pipeline automation.  

---

## 🔄 Project Workflow Overview

1. **Ingestion** – Daily car data is ingested (e.g., from Kaggle or raw CSVs).  
2. **Loading** – Data is loaded into a staging area (via Python scripts / Dagster ops).  
3. **Transformation** – DBT models clean, normalize, and transform raw data into structured models.  
4. **Validation** – DBT test suite ensures data quality and consistency.  
5. **Documentation** – Model documentation can be generated and served locally.  

---

## 🛠 Development Workflow

- **Iterate on DBT models** → run `dbt run` and `dbt test`.  
- **Extend ingestion** → add loaders in `kaggle_loader/` or `daily_data_loader/`.  
- **Add orchestration** → expand Dagster assets or schedules.  
- **Explore docs** → run `dbt docs serve` to see full lineage and schema details.  

---

## 🤝 Contributing

Contributions are welcome!  
1. Fork the repository.  
2. Create a topic branch: `git checkout -b feature/my-feature`  
3. Make your changes and add tests if applicable.  
4. Submit a Pull Request for review.  

---

## 📬 Contact

**Author**: Aleksandr Tanskii  
- [LinkedIn](https://www.linkedin.com/in/atanskiy/)  
- [GitHub](https://github.com/ATanskiy)  

---

## 🙏 Acknowledgments

- Structured to support real-world pipelines using **dbt + Dagster + Docker**.  

---

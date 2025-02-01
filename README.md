# Ethiopian-Medical-Data-Warehouse
A comprehensive data warehouse solution for Ethiopian medical business data, integrating Telegram scraping, image object detection using YOLO, and ETL/ELT pipelines for efficient data processing and analysis.


## Overview
The **Ethiopian Medical Data Warehouse** project aims to build a robust, scalable data warehouse that stores data on Ethiopian medical businesses scraped from the web and Telegram channels. The project integrates advanced data extraction techniques and object detection capabilities using YOLO (You Only Look Once) to enhance data analysis.

This repository will serve as a centralized hub for all project components, including data extraction scripts, ETL/ELT processes, object detection models, and more.

---

## Features
- **Data Scraping:** Collect data from public Telegram channels relevant to Ethiopian medical businesses.
- **Object Detection:** Utilize YOLO models for analyzing image data.
- **ETL/ELT Pipelines:** Efficient data transformation and storage strategies.
- **Data Storage:** Organize raw and processed data for analysis.
- **Monitoring and Logging:** Track progress and debug the scraping process.

---

---

## Getting Started
### Prerequisites
- **Python 3.8+**
- Install the necessary dependencies:

  ```bash
  pip install -r requirements.txt
  ```

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/Ethiopian-Medical-Data-Warehouse.git
   ```
2. Navigate to the project directory:
   ```bash
   cd Ethiopian-Medical-Data-Warehouse
   ```

---

## Usage
### Data Extraction
1. Update configuration files in `config/` with the required Telegram API keys and scraping settings.
2. Run the scraping script:
   ```bash
   python src/scraping/telegram_scraper.py
   ```

### ETL/ELT Pipelines
- To execute the data transformation pipeline, use:
  ```bash
  python src/etl/etl_pipeline.py
  ```

### Object Detection
- For running YOLO object detection on collected images:
  ```bash
  python src/object_detection/run_yolo.py
  ```

---

## Logging and Monitoring
- Log files are stored in the `logs/` directory.
- Scraping errors and processing status are automatically logged.

---

## Contribution Guidelines
Feel free to contribute by submitting issues or creating pull requests.

---

## Contact
For any inquiries or support, please reach out to [liuljima1896@gmail.com].


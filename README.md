# 🏎️ F1 Driver Performance & Consistency Analysis

A data analytics project exploring how driver consistency and performance interact — and what that means for decision-making in Formula 1.

---

## 🚀 Key Takeaways

- Built a composite **Consistency Score** to evaluate drivers beyond championship points
- Found a **weak relationship between consistency and performance**, highlighting a key strategic trade-off
- Identified that high-scoring drivers are often less consistent, challenging traditional rankings
- Delivered insights through an **interactive Power BI dashboard** for intuitive exploration

---

## 📊 Dashboard Preview

### Driver Consistency Ranking
<img width="1476" height="1032" alt="image" src="https://github.com/user-attachments/assets/a6a09678-6c3d-484d-bdfb-2b6cbf2480a1" />


### Consistency vs Performance — 4-Quadrant Analysis
<img width="2144" height="1360" alt="image" src="https://github.com/user-attachments/assets/ea6e5781-726e-42df-b41f-f7ab13166adc" />


### Teammate Comparison
<img width="2458" height="1192" alt="image" src="https://github.com/user-attachments/assets/eee413a5-940b-4885-b5d7-5a92f3b44d31" />


---

## 📌 Problem

Traditional F1 rankings focus on total points — but this ignores an important question:

> *Which drivers are consistently delivering performance, and how does consistency trade off against peak results?*

This project builds a framework to evaluate drivers across multiple dimensions, simulating how teams assess driver performance internally when making strategic decisions.

---

## 🧠 Approach

### 1. Data Pipeline
- Extracted lap-level and race-level data for the full 2025 F1 season via the FastF1 API
- Cleaned and validated the data — outlier removal, pit lap filtering, safety car lap exclusion, DNF handling
- Structured data into a relational SQLite database using SQLAlchemy for efficient querying

### 2. Metric Design

Developed a **Consistency Score** (0–1 scale) combining four dimensions:

| Dimension | What It Measures |
|---|---|
| Lap Consistency | Stability of lap times across the season |
| Position Consistency | Variability in race finishing positions |
| Teammate Comparison | Performance relative to the same-car driver |
| Reliability | Race completion rate (penalises DNFs) |

Also introduced a **Consistency-Performance Index (CPI)** — Consistency Score × Points Per Race — to identify drivers who balance both dimensions simultaneously.

---

## 📈 Key Insights

- **Consistency and performance are weakly correlated** — most drivers excel in one dimension but rarely both
- **High-performing drivers often sacrifice consistency** — aggressive driving strategies increase lap time variability
- **Teammate comparisons reveal hidden performance gaps** — controlling for car performance isolates true driver contribution
- **Reliability has a measurable impact on season output** — DNFs significantly drag down a driver's effectiveness score even when their pace is strong

---

## ⚙️ Tech Stack

| Tool | Role |
|---|---|
| Python (Pandas, NumPy) | Data extraction, cleaning, transformation, metric computation |
| FastF1 API | Source of lap timing, race results, and tyre data |
| SQL / SQLite + SQLAlchemy | Relational data storage and querying |
| Power BI | Interactive dashboard and visualisation |

---

## 💡 What This Project Demonstrates

- End-to-end ETL pipeline development
- Data cleaning and validation at scale
- Feature engineering and metric design
- Translating analysis into decision-support insights
- Building interactive dashboards for non-technical audiences

---

## ▶️ How to Run

```bash
pip install fastf1 pandas numpy sqlalchemy
python f1_pipeline.py
```

> First run downloads the full season from the F1 API (~30–60 min). All subsequent runs read from local cache and complete in seconds.

---

## 👤 Author

**Jasjot Singh**
University of Waterloo — BMath, Mathematical Studies (Minor: Computer Science)
[LinkedIn](https://linkedin.com/in/your-profile) | j367sing@uwaterloo.ca

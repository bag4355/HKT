
# ACCUTE: Accounting Compliance Cognitive Understanding Task Engine

ACCUTE is an AI-powered web application that assists accounting professionals in analyzing corporate disclosure documents and identifying similar financial reporting cases. It leverages Upstage API to process Korean business reports in PDF format and returns concise summaries and case comparisons to support compliance and decision-making tasks.

## Features

- PDF parsing and financial issue extraction  
- Intelligent summarization of key points using Upstage API  
- Similar case retrieval based on semantic similarity  
- Clean, responsive UI built with Tailwind CSS  
-  Easily extensible for future AI agent integration

## Project Structure

```
├── agi_pipeline.py         # Core logic using Upstage API for summarization & retrieval
├── app.py                  # Backend Flask app for API endpoints
├── index.html              # Frontend interface with file upload and response rendering
├── crawled_data.xlsx       # Sample case database for similarity search
```

## Installation

1. Clone this repository:

```bash
git clone https://github.com/your-username/accute.git
cd accute
```

2. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run the application:

```bash
python app.py
```

## Usage

1. Open `index.html` in your browser.
2. Enter your query in Korean (e.g., `"유사한 회계 보고서 찾아줘"`).
3. Upload a business report in PDF format.
4. Click the **실행하기** button.
5. View the AI-generated summary and similar cases side by side.

## Upstage API Integration

The `agi_pipeline.py` file uses the **Upstage API** to perform the following tasks:

- Summarize PDF contents
- Retrieve similar financial reporting cases based on vector similarity

These interactions are clearly marked within the code (see comments such as `# --- Upstage Summary API Call ---`).

## Testing & Evaluation

You can test the app by uploading the sample business report:
- `[가이아코퍼레이션]사업보고서(2025.03.31).pdf`

The app will process the document and return:
- A structured **summary**
- Relevant **similar case(s)** from the internal case database (`crawled_data.xlsx`)

## Dependencies

- Flask
- pandas
- PyMuPDF
- openai / Upstage API SDK (you must set your API key)

## License

This project is developed as part of an academic coursework submission and is currently not open-sourced under a specific license.

---

**Developed by:** Brave Team of Five  
**Affiliation:** [Your School or Course Name]

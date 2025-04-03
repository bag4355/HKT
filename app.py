from flask import Flask, request, jsonify, render_template
from werkzeug.utils import secure_filename
import os
from agi_pipeline import summarize_pdf_fully, rag_based_agi_pipeline

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
if not os.path.exists(app.config["UPLOAD_FOLDER"]):
    os.makedirs(app.config["UPLOAD_FOLDER"])

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/agi", methods=["POST"])
def api_agi():
    task_prompt = request.form.get("task_prompt", "")
    if "pdf_file" not in request.files:
        return jsonify({"error": "PDF 파일이 업로드되지 않았습니다."}), 400
    pdf_file = request.files["pdf_file"]
    if pdf_file.filename == "":
        return jsonify({"error": "파일 이름이 비어 있습니다."}), 400

    filename = secure_filename(pdf_file.filename)
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    pdf_file.save(file_path)

    try:
        summary = summarize_pdf_fully(file_path)
    except Exception as e:
        return jsonify({"error": f"PDF 요약 실패: {str(e)}"}), 500

    try:
        similar_case = rag_based_agi_pipeline(task_prompt, summary)
    except Exception as e:
        return jsonify({"error": f"AGI 파이프라인 실행 실패: {str(e)}"}), 500

    return jsonify({"summary": summary, "similar_case": similar_case})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

import openpyxl
from PyPDF2 import PdfReader
import concurrent.futures
import json
from typing import List
import cohere
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from openai import OpenAI
import re
import os

upstage_api_key = os.environ.get('upstage_api_key')
cohere_api_key = os.environ.get('cohere_api_key')
qdrant_api_key = os.environ.get('qdrant_api_key')
qdrant_host = os.environ.get('qdrant_host')
COLLECTION_NAME = os.environ.get('COLLECTION_NAME')

workbook = openpyxl.load_workbook('crawled_data.xlsx')
sheet = workbook.active

law_list = []
for cell in sheet['C']:
    value = str(cell.value) if cell.value is not None else ""
    law_list.append(value)

llm_client = OpenAI(
    api_key=upstage_api_key,
    base_url="https://api.upstage.ai/v1"
)
co = cohere.Client(cohere_api_key)
qdrant_client = QdrantClient(
    host=qdrant_host,
    port=6333,
    https=True,
    api_key=qdrant_api_key
)


def extract_text_from_pdf(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"
    return text


def split_text_by_length(text: str, chunk_size: int = 8000) -> list:
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]


def summarize_chunk(chunk: str) -> str:
    prompt = f"""다음은 회계 사업보고서의 일부입니다.

이 보고서에서 아래 항목들에 해당하는 중요한 회계 정보가 있다면 가능한 한 모두 포함하여 요약해 주세요.

- 회계정책변경 (회계정책 또는 회계추정 변경 내용)
- 수익인식_기준 (수익을 어떤 방식/시점으로 인식하는지)
- 조건부수익_변수보상 (성과급 등 조건부 수익 관련 처리)
- 계약변경 (계약 조건 변경과 회계영향)
- 진행률기준수익 (장기공사나 용역에 대한 수익 인식)
- 리스부외처리 (자산·부채 인식 제외된 리스 관련 사유)
- 충당부채_미인식 (우발채무 존재에도 불구하고 미인식된 사유)
- 정부보조금처리 (정부보조금 인식 및 처리 방식)
- 무형자산_자산화여부 (개발비 등 무형자산의 자산화 여부)
- 손상검사 (영업권, 투자자산 등의 손상검사 여부와 기준)
- 감가상각방법_변경 (감가상각 방식 또는 내용연수 변경 내용)

또한 기업의 배경을 파악할 수 있도록 다음 정보도 포함해 주세요:

- 산업분류
- 제품유형
- 수익구조
- 매출구성
- 고객유형
- 계약구조
- 연결대상여부 (연결 vs 개별 재무제표 기준)
- 회계기준 (K-IFRS, K-GAAP 등)
- 상장여부 (상장 or 비상장)

※ 가능한 항목이 많은 경우 요약이 길어져도 괜찮습니다.
※ 항목이 없으면 생략해도 됩니다.

\n\n{chunk}"""
    try:
        response = llm_client.chat.completions.create(
            model="solar-pro",
            messages=[
                {"role": "system", "content": "당신은 회계 문서를 요약할 때, 핵심 회계 이슈를 놓치지 않는 전문가입니다."},
                {"role": "user", "content": prompt}
            ],
            stream=False
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[요약 실패] {e}")
        return "[요약 실패]"


def summarize_in_chunks_parallel(text: str, max_workers: int = 6) -> list:
    chunks = split_text_by_length(text)
    print(f"[⚡] {len(chunks)}개의 chunk를 병렬로 요약 중...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        summaries = list(executor.map(summarize_chunk, chunks))
    return summaries


def merge_summaries(summaries: list) -> str:
    combined = "\n\n".join(summaries)
    prompt = f"""다음은 회계 보고서를 나눠서 요약한 내용입니다. 이들을 하나의 최종 요약으로 통합해 주세요:\n\n{combined}"""
    try:
        response = llm_client.chat.completions.create(
            model="solar-pro",
            messages=[
                {"role": "system", "content": "너는 회계 보고서를 통합 요약하는 전문가야."},
                {"role": "user", "content": prompt}
            ],
            stream=False
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[통합 요약 실패] {e}")
        return "[통합 요약 실패]"


def summarize_pdf_fully(pdf_path: str) -> str:
    print(f"[📄] PDF 텍스트 추출 중: {pdf_path}")
    raw_text = extract_text_from_pdf(pdf_path)
    print(f"[✂️] 텍스트 분할 + 병렬 요약 중...")
    chunk_summaries = summarize_in_chunks_parallel(raw_text, max_workers=6)
    print(f"[🧠] 통합 요약 생성 중...")
    final_summary = merge_summaries(chunk_summaries)
    return final_summary


def classify_task_with_llm(task_prompt: str) -> str:
    classification_prompt = f"""
우리가 처리할 수 있는 TASK는 아래 두 가지입니다:

1) "유사한 다른 회계 사업보고서 검색"
2) "입력 회계 사업보고서에서 사용된 법 찾아주기"

사용자의 요청: {task_prompt}

위 요청이 1번, 2번 중 어디에 가장 잘 해당하나요?
- 1 or 2 로만 답하세요.
- 둘 다 아니면 "None"이라고만 답하세요.
"""
    response = llm_client.chat.completions.create(
        model="solar-pro",
        messages=[
            {"role": "system", "content": "당신은 분류를 도와주는 어시스턴트입니다."},
            {"role": "user", "content": classification_prompt}
        ],
        stream=False
    )
    classification = response.choices[0].message.content.strip()
    if classification not in ["1", "2"]:
        classification = "None"
    return classification

def retrieve_qdrant_docs_by_ids(qdrant_client, collection_name: str, doc_ids: list):
    retrieved = qdrant_client.retrieve(
        collection_name=collection_name,
        ids=doc_ids
    )
    return retrieved

def search_and_rerank_with_indexing(qdrant_client, co, query_vector, collection_name):
    results = qdrant_client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=5
    )
    candidates = []
    for r in results:
        candidates.append({
            "id": r.id,
            "score": r.score,
            "payload": r.payload
        })

    documents = [c["payload"]["text"] for c in candidates]
    rerank_response = co.rerank(
        query="유사한 회계 사업보고서 찾기",
        documents=documents,
        top_n=len(documents),
        model="rerank-multilingual-v3.0"
    )
    reranked_indices = sorted(
        rerank_response.results,
        key=lambda x: x.relevance_score,
        reverse=True
    )

    reranked_candidates = []
    for i, r in enumerate(reranked_indices):
        original_idx = r.index
        reranked_candidates.append({
            "report_index": i,
            "id": candidates[original_idx]["id"],
            "score": candidates[original_idx]["score"],
            "payload": candidates[original_idx]["payload"]
        })
    return reranked_candidates

def retrieve_by_report_index(report_index: int, reranked_candidates, qdrant_client, collection_name):
    match_candidates = [c for c in reranked_candidates if c["report_index"] == report_index]
    if not match_candidates:
        print(f"[역추적 실패] report_index={report_index} 에 해당하는 문서가 없습니다.")
        return None

    qdrant_id = match_candidates[0]["id"]
    print(f"[역추적] report_index={report_index} → Qdrant ID={qdrant_id}")

    retrieved_docs = retrieve_qdrant_docs_by_ids(qdrant_client, collection_name, [qdrant_id])
    for doc in retrieved_docs:
        print(f"[역추적 결과] ID={doc.id}, payload={doc.payload}")
        return doc
    return None

def get_solar_embedding(text: str) -> List[float]:
    response = llm_client.embeddings.create(
        input=text,
        model="embedding-query"
    )
    embedding_vector = response.data[0].embedding
    return embedding_vector

def generate_final_answer_with_llm(query: str, contexts: List[str]) -> str:
    combined_context = "\n".join(contexts)
    system_prompt = (
        "당신은 숙련된 회계 전문가입니다. 아래는 하나의 회계 사업보고서와 유사한 보고서들의 요약입니다. "
        "이 텍스트들을 참고하여 어떤 보고서가 가장 유사한지 판단하고, 그 이유를 상세히 설명해 주세요.\n\n"
        "유사성 판단 기준에는 다음 요소들을 고려하세요:\n"
        "- 회계기준 적용 (K-IFRS, K-GAAP 등)\n"
        "- 수익 인식 방식\n"
        "- 충당부채 처리 방식\n"
        "- 무형자산 인식 및 자산화 여부\n"
        "- 감가상각 방법 및 내용연수\n\n"
        "최종 출력은 반드시 다음 JSON 형식을 따르세요:"
    )
    user_prompt = f"""
[보고서 컨텍스트]
{combined_context}

[사용자 질문]
{query}

[요청]
- 입력 보고서와 가장 유사한 보고서를 1개 선택하고, 그 유사한 이유를 명확히 작성해 주세요.

[출력 형식 예시]
{{
  "유사한_보고서_ID": "보고서 4",
  "유사한_이유": "두 보고서는 모두 K-IFRS를 적용하고 있으며, 수익 인식, 충당부채, 무형자산 회계처리 방식이 유사합니다."
}}
"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    response = llm_client.chat.completions.create(
        model="solar-pro",
        messages=messages,
        stream=False,
    )
    return response.choices[0].message.content

def postprocess_final_answer_with_company_name(final_answer: str, reranked_candidates: list) -> str:
    """
    LLM 결과의 "유사한_보고서_ID": "보고서 3" → 실제 filename 등으로 바꾸는 함수
    """
    try:
        parsed = json.loads(final_answer)
        report_id = parsed.get("유사한_보고서_ID", "")
        if report_id.startswith("보고서 "):
            report_index = int(report_id.replace("보고서 ", ""))
            for c in reranked_candidates:
                if c["report_index"] == report_index:
                    fname = c["payload"].get("filename", "")
                    match = re.search(r"^(\[.*?\].*?\(\d{4}\.\d{2}\.\d{2}\))", fname)
                    if match:
                        extracted = match.group(1)
                        parsed["유사한_보고서_ID"] = extracted
                        final_answer = json.dumps(parsed, ensure_ascii=False)
                        break
    except Exception as e:
        print(f"[후처리 오류] {e}")
    return final_answer

def rag_based_agi_pipeline(task_prompt: str, report_1: str, report_2: str = "") -> str:
    task_type = classify_task_with_llm(task_prompt)
    if task_type == "1":
        query = "유사한 회계 사업보고서 찾기"
        query_vector = get_solar_embedding(query)
        reranked_candidates = search_and_rerank_with_indexing(qdrant_client, co, query_vector, COLLECTION_NAME)
        top_contexts = [item["payload"]["text"] for item in reranked_candidates[:3]]
        final_answer = generate_final_answer_with_llm(query, top_contexts)

        final_answer = postprocess_final_answer_with_company_name(final_answer, reranked_candidates)

        final_answer = re.sub(r"[{}]", "", final_answer)

        return final_answer

    elif task_type == "2":
        combined_reports = f"[보고서1]\n{report_1}\n\n[보고서2]\n{report_2}"
        system_prompt = (
            "당신은 K-IFRS 및 회계기준 전문 어시스턴트입니다. "
            "다음은 2개의 회계 사업보고서입니다. "
            "각 보고서에서 실제로 언급된 회계기준서(K-IFRS)와 법령을 식별하고, 그 내용을 정리해 주세요.\n\n"
            "⚠️ 지침:\n"
            "- 실제 보고서에 명시적으로 언급된 기준서 또는 법령만 포함해 주세요.\n"
            "- 기준서는 '기업회계기준서 제XXXX호'처럼 표기된 항목만 추출합니다.\n"
            "- 법령은 '법', '시행령', '규정' 등의 정식 명칭으로 식별합니다.\n"
            "- 관련 기준/법령이 언급된 문장은 최대 5개까지 포함해 주세요.\n"
            "- 출력은 반드시 JSON 형식으로만 반환하며, 다른 설명은 포함하지 마세요."
        )
        user_prompt = f"""
[보고서 1]
{report_1}

[보고서 2]
{report_2}

요청: 위 두 보고서를 기반으로 아래 JSON 형식에 맞게 회계기준서 및 법령 정보를 정리하세요.

json
{{
    "회계기준서_적용": [ ... ],
    "관련_법령": [ ... ],
    "회계기준_관련_문장": [ ... ]
}}
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        response = llm_client.chat.completions.create(
            model="solar-pro",
            messages=messages,
            stream=False
        )

        final_answer = response.choices[0].message.content
        final_answer = re.sub(r"[{}]", "", final_answer)
        return final_answer
    else:
        return "요청한 TASK를 수행할 수 없습니다."

# appsscript-gamilewaver

# GmailWeaver

## Overview

**GmailWeaver**는 Gmail 이메일 데이터를 분석하여 **지식 그래프(Knowledge Graph)를 구축**하고, **GraphRAG 기반 자연어 검색**을 제공하는 시스템이다.

사용자는 Gmail Add-on 인터페이스를 통해 자연어로 질의를 입력할 수 있으며, 서버는 이메일에서 추출된 **엔티티**(Entity)와 **관계**(Relation)를 기반으로 구축된 그래프를 탐색하여 관련 정보를 반환한다.

본 시스템은 다음과 같은 구조로 구성된다.

- **클라이언트** : Gmail Add-on (Google Apps Script 기반)
- **서버** : GraphRAG 기반 질의 처리 서버 (Flask)
- **데이터 계층** : 이메일로부터 구축된 Knowledge Graph

또한 이메일에서 일정 정보를 추출하여 **Google Calendar API와 연동**하고, 이메일 간 관계를 **그래프 형태로 시각화**하여 사용자에게 제공하는 기능을 목표로 한다.

---

## Features

- Gmail 이메일 **엔티티 및 관계 추출**
- 이메일 기반 **Knowledge Graph 생성**
- **GraphRAG 기반 자연어 질의 검색**
- Gmail Add-on UI 기반 검색 인터페이스
- 이메일 관계 **그래프 시각화**
- 이메일 일정 **Google Calendar 자동 등록**

---

## Architecture

```text
Gmail Add-on (Apps Script)
    │
    │ HTTP request / response
    ▼
Flask Server
    ├─ mail2json / preprocessing
    ├─ GraphRAG query / indexing
    ├─ Knowledge Graph (graph.graphml)
    └─ Vector DB / index storage
```

---

# GmailWeaver Development Guide 

## Python Environment

**GraphRAG 2.1.0은 Python 3.11 환경에서 사용하는 것을 권장한다.**
Python 3.12+ 또는 3.13 환경에서는 설치/실행 호환성 문제가 발생할 수 있다.

### Virtual Environment 생성

```bash
py -3.11 -m venv gmailweaver-venv
```

### 가상환경 활성화
```bash
./gmailweaver-venv/Scripts/activate
```
### Python 환경 확인
```bash
which python
```
프로젝트 내 가상환경 경로가 출력되면 정상적으로 활성화된 것이다.

### 가상환경 내 GraphRAG 설치
```bash
pip install graphrag==2.1.0
```

---

## Required Libraries

다음 라이브러리를 사용한다.
- openai: OpenAI API 호출
- python-dotenv: 환경 변수(.env) 로드
- Flask: 서버 프레임워크
- flask-cors: CORS 설정
- PyMuPDF: PDF 읽고 텍스트 추출 
- python-docx: .docx 파일 읽고 수정
(- pandas: 데이터 분석)

### 설치 및 확인
```bash
pip install openai python-dotenv Flask flask-cors PyMuPDF python-docx pandas
pip list
```
---

## Apps Script CLI (clasp)
Gmail Add-on 개발을 위해 **clasp CLI**를 사용한다.
```bash
clasp login
clasp clone <script-id>
```

### 주요 명령어
```bash
clasp push    # 로컬 → Apps Script 반영
clasp pull    # Apps Script → 로컬 반영
```
---

# Run

## 1. ngrok 실행

로컬 서버를 외부에서 접근할 수 있도록 ngrok을 실행한다.

```bash
ngrok http 80
```
이때 생성된 Forwarding URL이 자신의 터널링 주소이다.

## 2. Apps Script 설정 수정

다음 파일에서 자신의 환경에 맞게 주소를 수정한다.

### common.json
- 'TunnelURL'을 자신의 **Tunnerling URL**로 변경
- 'WEBAPP_URL'을 자신의 **Web App URL**로 변경

### appsscript.json
- 'urlFetchWhitelist'을 자신의 **Tunnerling URL**로 변경
이때 맨 끝에 '/' 문자를 반드시 추가하여 "...ngrok-free.dev/"와 같은 형태로 만든다.

---

## 3. GraphRAG 서버 실행

프로젝트 루트에서 서버를 실행한다.
```bash
python src/app.py
```
서버가 정상 실행되면 Flask 서버가 localhost에서 동작한다.

---

## 4. Apps Script 코드 반영

./src/apps-script 디렉토리에서 다음 명령어를 실행한다.
```bash
clasp push
```
로컬에서 수정한 코드가 Apps Script 프로젝트에 반영된다.

---

## 5. Gmail Add-on 테스트

1. Apps Script Console에서 프로젝트 열기
2. Test Deployment 실행
3. Gmail을 열어 Add-on을 테스트

---

# Prerequisites

- Python 3.11
- Node.js
- ngrok
- clasp
- Gmail 계정

from fastapi import FastAPI

app = FastAPI(
    title="ProcureLens Intelligence Agent",
    description="Agent 3: FastAPI Orchestration & AI Layer",
    version="1.0.0"
)

@app.get("/")
def root():
    return {"message": "FastAPI Agent 3 is running!"}

import uvicorn


if __name__ == "__main__":
    uvicorn.run("dqc.main:app", host="127.0.0.1", port=8090, log_level="info", workers=1)

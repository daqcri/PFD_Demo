from python:3.7

copy . /app
run pip install -r /app/requirements.txt
workdir /app
entrypoint ["python", "app.py"]

FROM python:3.7

# creates directory path in container
WORKDIR /app/web_app_dir

# <src on machine running dockerile> <dest inside container>, . is WORKDIR
COPY requirements.txt .

# <src> <dest> same as copy with extra functionalities, . is WORKDIR
ADD web_app.py .

RUN pip install -r requirements.txt

# expose done in docker compose
# EXPOSE 8050

CMD ["python", "web_app.py"]

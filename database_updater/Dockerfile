FROM python:3.7

# creates directory path in container
WORKDIR /app/db_updater_dir

# <src on machine running dockerile> <dest inside container>, . is WORKDIR
COPY requirements.txt .
COPY constants.py .

# <src> <dest> same as copy with extra functionalities, . is WORKDIR
ADD db_updater.py .

RUN pip install -r requirements.txt

# expose done in docker compose
# EXPOSE 8050

CMD ["python", "db_updater.py"]

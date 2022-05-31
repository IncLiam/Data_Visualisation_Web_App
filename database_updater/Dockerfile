FROM python:3.7

COPY requirements.txt /app/db_updater_dir/requirements.txt
COPY constants.py /app/db_updater_dir/constants.py

WORKDIR /app/db_updater_dir

ADD db_updater.py .

RUN pip install -r requirements.txt

# EXPOSE 8050

CMD ["python", "db_updater.py"]

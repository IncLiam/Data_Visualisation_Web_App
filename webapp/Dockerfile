FROM python:3.7

COPY requirements.txt /app/webapp_dir/requirements.txt

WORKDIR /app/webapp_dir

ADD web_app.py .

RUN pip install -r requirements.txt

# EXPOSE 8050

CMD ["python", "web_app.py"]

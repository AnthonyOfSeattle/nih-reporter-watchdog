from python:3.12-bullseye

COPY ./requirements.txt /
RUN pip3 install -r requirements.txt

WORKDIR "/app"

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]

FROM python:2.7.13

COPY . /opt/app
WORKDIR /opt/app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","./automationEagle.py"]
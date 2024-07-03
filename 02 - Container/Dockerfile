FROM python:3.11.9-slim-bullseye

WORKDIR /deployments
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]

# docker build -t myimage:0.01 . 
# docker create -e URL='https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html' -e CONNECTION_STRING='postgresql://postgres:mysecretpassword@192.168.0.26/postgres' --name mycontainer myimage:0.01

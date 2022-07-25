FROM continuumio/miniconda3
WORKDIR /var/lib/Aggregator
COPY ./environment.yml .
RUN conda env create --name myenv --file environment.yml
COPY ./App ./App
COPY aggregator.py .
COPY __init__.py .
WORKDIR /var/lib
SHELL ["conda", "run", "-n", "myenv"]
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "myenv", "python3", "-m", "Aggregator.aggregator"]
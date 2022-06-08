FROM continuumio/miniconda3
WORKDIR /app

COPY ./environment.yml .
RUN conda env create --name myenv --file environment.yml
COPY . .
SHELL ["conda", "run", "-n", "myenv", "bin/bash", "-c"]
ENTRYPOINT ["entrypoint.sh"]




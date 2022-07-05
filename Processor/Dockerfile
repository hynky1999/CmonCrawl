FROM continuumio/miniconda3
WORKDIR /app

COPY ./environment.yml .
RUN conda env create --name myenv --file environment.yml
COPY . .
SHELL ["conda", "run", "-n", "myenv"]
# RUN pip install -r requirements.txt
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "myenv", "python", "processor.py"]
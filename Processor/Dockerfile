FROM continuumio/miniconda3
WORKDIR /var/lib/Processor
COPY ./environment.yml .
RUN conda env create --name myenv --file environment.yml
COPY ./App ./App
COPY processor.py .
COPY __init__.py .
WORKDIR /var/lib
SHELL ["conda", "run", "-n", "myenv"]
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "myenv", "python3", "-m" , "Processor.processor", "--output_path=/output"]
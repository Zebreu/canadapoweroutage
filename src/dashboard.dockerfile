FROM continuumio/miniconda3:4.12.0

RUN conda install psycopg2=2.9.3

COPY requirements.txt .

RUN pip install -f requirements.txt

WORKDIR /src/app

COPY dashboard.py /src/app

CMD streamlit run /src/app/dashboard.py --browser.gatherUsageStats=False --theme.base="dark"
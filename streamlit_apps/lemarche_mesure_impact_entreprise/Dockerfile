FROM python:3.12-slim

WORKDIR /app

COPY streamlit_apps/lemarche_mesure_impact_entreprise/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir uv
RUN uv pip install --system --no-cache -r /app/requirements.txt

COPY streamlit_apps/commun /app/streamlit_apps/commun
COPY streamlit_apps/lemarche_mesure_impact_entreprise /app/streamlit_apps/lemarche_mesure_impact_entreprise

ENV PYTHONPATH=/app

CMD ["bash", "-lc", "streamlit run streamlit_apps/lemarche_mesure_impact_entreprise/app.py --server.address=0.0.0.0 --server.port=${PORT:-8501}"]

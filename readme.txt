docker build -t facebook-chatbot-backend .

docker run -d \
  -p 8000:8000 \
  --env-file .env \
  --name fb-chatbot \
  facebook-chatbot-backend


  uvicorn app:app --reload
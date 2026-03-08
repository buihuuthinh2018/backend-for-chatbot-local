docker build -t facebook-chatbot-backend .

docker run -d \
  -p 8082:8082 \
  --env-file .env \
  --name fb-chatbot \
  facebook-chatbot-backend


  uvicorn app:app --reload
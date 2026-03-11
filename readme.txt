docker build -t facebook-chatbot-backend .

docker run -d \
  -p 8082:8082 \
  --env-file .env \
  --name fb-chatbot \
  facebook-chatbot-backend


  uvicorn app:app --reload


  Chay kenh mqtt tren docker 
  docker run -it -p 1883:1883 -p 9001:9001 eclipse-mosquitto
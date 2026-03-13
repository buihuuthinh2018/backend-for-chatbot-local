# 1️⃣ Base image nhẹ
FROM python:3.11-slim

# 2️⃣ Tắt bytecode và bật log realtime
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MONGO_URI=mongodb://mongodb:27017
ENV BACKEND_DB_NAME=chatbot_backend

# 3️⃣ Tạo thư mục làm việc
WORKDIR /app

# 4️⃣ Copy requirements trước để tận dụng cache layer
COPY requirements.txt .

# 5️⃣ Cài dependency
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 6️⃣ Copy source code
COPY . .

# 7️⃣ Expose port
EXPOSE 8082

# 8️⃣ Chạy server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8082"]
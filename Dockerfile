FROM python:3.11-slim

WORKDIR /app

# 关键：只复制依赖文件 → 变更代码不会触发pip重装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制所有代码
COPY . .

# 保持使用main.py
CMD ["python", "main.py"]

FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# 默认启动控制服务；compose 中设备模拟器覆盖此命令
CMD ["uvicorn", "app.control.main:app", "--host", "0.0.0.0", "--port", "8000"]

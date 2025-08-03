# Redis + Python + Docker: Producer & Consumer

Este proyecto demuestra cómo usar **Redis como sistema de mensajería** para conectar un **Producer** y un **Consumer**, ambos escritos en **Python**, usando contenedores **Docker** orquestados con **Docker Compose**.

---

## 🧱 Estructura del Proyecto

├── docker-compose.yml # Orquesta Redis, Producer y Consumer
├── producer/
│ ├── Dockerfile # Construye el contenedor del Producer
│ ├── app.py # Lógica del Producer
│ └── requirements.txt # Dependencias del Producer
├── consumer/
│ ├── Dockerfile # Construye el contenedor del Consumer
│ ├── app.py # Lógica del Consumer
│ └── requirements.txt # Dependencias del Consumer

yaml
Copy
Edit

---

## 🚀 Cómo ejecutar el proyecto

Asegúrate de tener [Docker](https://www.docker.com/) y [Docker Compose](https://docs.docker.com/compose/) instalados.

### 1. Clona el repositorio

```bash
git clone https://github.com/tu_usuario/redis-docker-python.git
cd redis-docker-python
2. Ejecuta con Docker Compose
bash
Copy
Edit
docker-compose up --build
Esto iniciará tres contenedores:

🟥 Redis

🟦 Producer (Python)

🟩 Consumer (Python)

⚙️ ¿Cómo funciona?
El Producer genera mensajes (mensaje 0, mensaje 1, ...) y los envía a Redis usando RPUSH sobre una lista llamada cola.

El Consumer escucha esa lista usando BLPOP, y consume los mensajes uno a uno.

Redis actúa como una cola FIFO (First-In First-Out) entre ambos.

🧪 Salida esperada
text
Copy
Edit
producer_1  | Producer: enviado -> mensaje 0
consumer_1  | Consumer: recibido -> mensaje 0
producer_1  | Producer: enviado -> mensaje 1
consumer_1  | Consumer: recibido -> mensaje 1
...
🧼 Para detener los contenedores
Presiona Ctrl+C y luego:

bash
Copy
Edit
docker-compose down
📦 Tecnologías usadas
Python 3.11
Librería redis-py
Redis 7

Docker / Docker Compose

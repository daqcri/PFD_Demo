FROM python:3.7.0

# Set working directory
WORKDIR /app

# Add and install requirements
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app
COPY . /app

# Run server
CMD ["python", "app.py"]

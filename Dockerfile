# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables to prevent Python from writing pyc files and to enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /src

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project (including the 'src' directory)
COPY . .

# DON'T copy data directory - it will be mounted as a volume
# This line is removed: COPY data/ ./data/

# Copy the setup.py file and install your project in "editable" mode
# This means changes to your source code will be reflected in the container
COPY setup.py .
RUN pip install -e .

# Create data directory (will be used by volume mount)
RUN mkdir -p /src/data

# Use CMD to run your application when the container starts
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8502"]

# Use an official Python runtime as a parent image
FROM alpine:3.7
FROM python:3-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt


# Run app.py when the container launches
CMD ["python", "spider.py"]

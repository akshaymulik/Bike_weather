#Using base image
FROM python:3.9-slim

#Copy files
WORKDIR /app
COPY . .
EXPOSE 8080

#Install dependencies
RUN pip install -r requirements.txt

ENV PORT 8080

CMD [ "python", "./sub.py" ] 
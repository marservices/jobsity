# Using lightweight alpine image
FROM python:3.9.13

# Installing packages
#RUN apk update
RUN pip install --no-cache-dir pipenv

# Defining working directory and adding source code
WORKDIR /jobsity
COPY Pipfile Pipfile.lock bootstrap.sh ./

RUN mkdir ./trips
COPY . .

# Install API dependencies
RUN pipenv install --system --deploy

# Start app
EXPOSE 8000
ENTRYPOINT ["/bootstrap.sh"]
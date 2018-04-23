# Use an official Python runtime as a parent image
FROM mdillon/postgis:10

# Make port 80 available to the world outside this container
EXPOSE 5432

# Define environment variable
ENV POSTGRES_USER postgres
ENV POSTGRES_DB postgres
ENV POSTGRES_PASSWORD=test_admin

COPY ./docker_provision/init_db.sh /docker-entrypoint-initdb.d/
COPY ./example_data/batch3dfier_db.sql .


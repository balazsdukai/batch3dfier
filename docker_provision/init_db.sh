#!/bin/bash
set -e

psql -p 5432 -h localhost -U postgres -d postgres -c "create role batch3dfier with login password 'batch3d_test';"

psql -p 5432 -h localhost -U postgres -d postgres -c "create database batch3dfier_db with owner batch3dfier;"

psql -p 5432 -h localhost -U postgres -d batch3dfier_db -c "create extension postgis;"

psql -p 5432 -h localhost -U postgres -d batch3dfier_db -f ./batch3dfier_db.sql

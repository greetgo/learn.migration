#!/bin/sh

export PG_ADMIN_USERID=postgres
export PG_ADMIN_PASSWORD=Super_Secret
export PG_ADMIN_URL=jdbc:postgresql://localhost/postgres

gradle dropCreateOperDb dropCreateCiaDb

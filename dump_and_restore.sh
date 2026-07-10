#!/bin/bash

pg_dump -d bildzitate -h localhost -p 5433 -U  bildzitate -c -f bildzitate_dump.sql
psql -U postgres -d bildzitate < bildzitate_dump.sql

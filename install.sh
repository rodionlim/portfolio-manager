#!/bin/bash

# This script creates and seeds the tables necessary for Portfolio Manager.
# New users should modify the templates according to the assets that they trade before running this
# Run it at the root of pf_manager

set -e

echo "Seeding database tables"
./scripts/seed_tables.py

echo "Computing dividends"
./portfolio/portfolio.py
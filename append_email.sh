#!/bin/bash

# Check if an email argument is passed
if [ -z "$1" ]; then
  echo "Please provide an email address."
  exit 1
fi

# Append the provided email to the file
echo "$1" >> failed-retrying-emails.txt
cat failed-retrying-emails.txt

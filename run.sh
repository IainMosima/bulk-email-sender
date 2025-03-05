#!/bin/bash

# Default to it if no environment is specified
ENV=${1:-it}

echo "Running bulk email sender with environment: $ENV"
python bulk-email-sender.py --env $ENV

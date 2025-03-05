#!/bin/bash

# Default to pension if no environment is specified
ENV=${1:-it}

echo "Running bulk email sender with environment: $ENV"
python bulk-email-sender.py --env $ENV


# Python script path
SCRIPT = bulk-email-sender.py

# Default target shows help information
help:
	@echo "Bulk Email Sender"
	@echo "---------------"
	@echo "Available commands:"
	@echo "  make it-send        - Send emails using IT environment"
	@echo "  make pension-send   - Send emails using Pension environment"
	@echo "  make governance-send - Send emails using Governance environment"
	@echo "  make clean          - Remove log files"

# Environment-specific targets
it-send:
	python $(SCRIPT) --env it

pension-send:
	python $(SCRIPT) --env pension

governance-send:
	python $(SCRIPT) --env governance

# Clean target
clean:
	rm -f *.log

# Phony targets (targets that don't represent files)
.PHONY: help it-send pension-send governance-send clean

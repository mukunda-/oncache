#/////////////////////////////////////////////////////////////////////////////////////////
#// oncache (C) 2025 Mukunda Johnson (mukunda.com)
#// Licensed under MIT. See LICENSE file.
#/////////////////////////////////////////////////////////////////////////////////////////

.PHONY: all cover

all:
	@echo "Nothing to do. This project does not have artifacts."

# Generate test coverage report coverage.html
cover:
	go test -v -coverprofile coverage ./...
	go tool cover -html coverage -o coverage.html
	
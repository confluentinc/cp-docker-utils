.PHONY: go-test go-format go-format-check
go-test:
	@echo "Running Go tests..."
	go test -v ./...

go-fmt:
	@echo "Formatting Go files..."
	go fmt ./...

go-check-fmt:
	@echo "Checking Go formatting..."
	@if [ -n "$$(gofmt -l .)" ]; then \
		echo "The following files are not properly formatted:"; \
		gofmt -l .; \
		exit 1; \
	else \
		echo "All Go files are properly formatted"; \
	fi
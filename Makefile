MODULE := github.com/refreshTechRusuh/booking-service

.PHONY: run build test lint golint gosec gen-mocks mod-tidy

run:
	go run cmd/main.go

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/api cmd/main.go

test:
	go test -v ./...

test-race:
	go test -race ./...

unit-test-coverage:
	go test -v -covermode=count $$(go list ./... | grep -v '/test/') -coverprofile=coverage.cov
	go tool cover -func=coverage.cov

mod-tidy:
	go mod tidy

golint:
	golangci-lint run --timeout 5m

gosec:
	gosec -exclude=G401,G304,G501,G505 -fmt=sonarqube -out=sonar-gosec.json ./...

# Mock generation — jalankan setelah mengubah interface di port/
gen-mock-booking-inbound:
	mockgen -package=mock_inbound \
		-destination=_mock/booking/port/inbound/booking_mock.go \
		-source=internal/booking/port/inbound/booking.go

gen-mock-booking-outbound:
	mockgen -package=mock_outbound \
		-destination=_mock/booking/port/outbound/ports_mock.go \
		-source=internal/booking/port/outbound/ports.go

gen-mocks: gen-mock-booking-inbound gen-mock-booking-outbound

check-mocks:
	go build ./_mock/booking/port/inbound/ ./_mock/booking/port/outbound/

docker-build: build
	docker build -f Dockerfile -t booking-service:latest .

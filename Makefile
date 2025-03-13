CURRENT_DIR := $(CURDIR)
COMPOSE_FILE := ${CURRENT_DIR}/docker-compose.yaml
LOGS_DIR := ${CURRENT_DIR}/logs

.PHONY: setup

create-used-paths:
	@mkdir -p ${LOGS_DIR}

remove-used-paths:
	@rm -rf ${LOGS_DIR}

create-user-id:
	@echo "AIRFLOW_UID=$(shell id -u)" > ${CURRENT_DIR}/.env

setup: create-used-paths create-user-id
	@docker-compose -f ${COMPOSE_FILE} build

stop:
	@docker-compose -f ${COMPOSE_FILE} down

start: stop
	@docker-compose -f ${COMPOSE_FILE} up -d

clean: remove-used-paths create-used-paths

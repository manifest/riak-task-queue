#!/bin/bash

PROJECT='riak-task-queue'
PROJECT_DIR="/opt/sandbox/${PROJECT}"
DOCKER_CONTAINER_NAME="sandbox/${PROJECT}"
DOCKER_CONTAINER_COMMAND=${DOCKER_CONTAINER_COMMAND:-'/bin/bash'}
ULIMIT_FD=262144

function PROPS() {
	local INDEX_NAME="${1}"
	local BUCKET_OPTIONS="${2}"
	if [[ ${BUCKET_OPTIONS} ]]; then
		echo "{\"props\":{\"search_index\":\"${INDEX_NAME}\",${BUCKET_OPTIONS}}}"
	else
		echo "{\"props\":{\"search_index\":\"${INDEX_NAME}\"}}"
	fi
}

function CREATE_TYPE() {
	local HOST='http://localhost:8098'
	local SCHEMA_NAME="${1}"
	local INDEX_NAME="${1}_idx"
	local TYPE_NAME="${1}_t"
	local BUCKET_OPTIONS="${2}"
	read -r RESULT <<-EOF
		curl -fSL \
			-XPUT "${HOST}/search/schema/${SCHEMA_NAME}" \
			-H 'Content-Type: application/xml' \
			--data-binary @"${PROJECT_DIR}/priv/riak-kv/schemas/${SCHEMA_NAME}.xml" \
		&& curl -fSL \
			-XPUT "${HOST}/search/index/${INDEX_NAME}" \
			-H 'Content-Type: application/json' \
			-d '{"schema":"${SCHEMA_NAME}"}' \
		&& riak-admin bucket-type create ${TYPE_NAME} '$(PROPS ${INDEX_NAME} ${BUCKET_OPTIONS})' \
		&& riak-admin bucket-type activate ${TYPE_NAME}
	EOF
	echo "${RESULT}"
}

read -r DOCKER_RUN_COMMAND <<-EOF
	service rsyslog start \
	&& riak start \
	&& riak-admin wait-for-service riak_kv \
	&& $(CREATE_TYPE riaktq_task '"datatype":"map"')
EOF

docker build -t ${DOCKER_CONTAINER_NAME} .
docker run -ti --rm \
	-v $(pwd):${PROJECT_DIR} \
	--ulimit nofile=${ULIMIT_FD}:${ULIMIT_FD} \
	-p 8098:8098 \
	-p 8087:8087 \
	-p 8093:8093 \
	-p 8985:8985 \
	${DOCKER_CONTAINER_NAME} \
	/bin/bash -c "set -x && cd ${PROJECT_DIR} && ${DOCKER_RUN_COMMAND} && set +x && ${DOCKER_CONTAINER_COMMAND}"


#!/usr/bin/env bash
# Aggregate gate: assert the required CI jobs reached their expected states.
# Reads CHANGES_RESULT / QUALITY_RESULT / TEST_RESULT / COVERAGE_RESULT and
# CODE / FORCE_FULL from the environment.
set -euo pipefail

echo "changes.result=${CHANGES_RESULT}"
echo "quality.result=${QUALITY_RESULT}"
echo "test.result=${TEST_RESULT}"
echo "coverage.result=${COVERAGE_RESULT}"
echo "code=${CODE}"
echo "force_full=${FORCE_FULL}"

if [[ "${CHANGES_RESULT}" != "success" ]]; then
	echo "changes job failed"
	exit 1
fi

if [[ "${FORCE_FULL}" == "true" || "${CODE}" == "true" ]]; then
	if [[ "${QUALITY_RESULT}" != "success" ]]; then
		echo "quality job did not succeed"
		exit 1
	fi
	if [[ "${TEST_RESULT}" != "success" ]]; then
		echo "test matrix did not succeed"
		exit 1
	fi
	if [[ "${COVERAGE_RESULT}" != "success" ]]; then
		echo "coverage gate did not succeed"
		exit 1
	fi
else
	if [[ "${QUALITY_RESULT}" != "skipped" ]]; then
		echo "quality was expected to be skipped"
		exit 1
	fi
	if [[ "${TEST_RESULT}" != "skipped" ]]; then
		echo "test was expected to be skipped"
		exit 1
	fi
	if [[ "${COVERAGE_RESULT}" != "skipped" ]]; then
		echo "coverage was expected to be skipped"
		exit 1
	fi
fi

echo "required-ci passed"

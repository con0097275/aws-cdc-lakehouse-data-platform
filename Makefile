# Portfolio repository — the two gates that apply to what is published here.
#
# The full platform's Makefile had 71 targets driving Spark, dbt, Terraform, Athena and the
# AI layer. Those targets are not in this repository because the code they drive is not
# (PORTFOLIO_SCOPE.md). A Makefile advertising 59 targets that cannot run is worse than a
# short one that works, so this is the short one.

.DEFAULT_GOAL := help
.PHONY: help validate-docs check-links test check

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

validate-docs: ## Cross-document consistency checks. No network, $$0.
	@python3 scripts/validate-docs.py

check-links: ## Every relative Markdown link and anchor. No network, $$0.
	@python3 scripts/check-links.py

test: ## 652 tests over everything published. No Spark cluster, no Kafka, no AWS. $$0.
	@python3 -m pytest spark/tests/ -q

check: validate-docs check-links test ## Everything above, in order.
	@echo "all gates passed"

# Security

This is a **portfolio and demonstration repository**. It contains no production credentials,
no customer data, and no access to any live system.

## What is in here

* Infrastructure-as-code, pipeline code, tests, and documentation.
* Synthetic data only. The banking domain (accounts, customers, transactions) is generated
  by the source lab; no real records of any kind are present.
* Placeholder identifiers. The AWS account id is `111122223333`, AWS's own documentation
  placeholder; the CLI profile and email are stand-ins. See
  [`SANITIZATION.md`](SANITIZATION.md).

## What is deliberately not in here

No credentials of any kind. The platform's design keeps secrets in AWS Secrets Manager and
SSM Parameter Store and reads them at runtime, so even the saved Terraform plans show
parameter **paths** rather than values. Terraform state, plan JSON, `.tfvars`, `.env` files
and run logs are excluded from this copy.

## If you think you have found an exposure

Open an issue describing **the class of problem and the file**, without pasting the suspected
secret into a public issue. If you believe it is a live credential, say so in the first line
so it can be triaged first.

Things worth reporting even in a demo repository:

* anything that looks like a real account id, access key, private key, or connection string
* a real hostname, endpoint, or bucket name
* personal data of any kind

## Running it yourself

The identity guard in `scripts/lib.sh` asserts an expected AWS account and profile before any
AWS call, and those values are placeholders here — so every script will **stop** rather than
act against an account it was not built for. That is deliberate. Set your own values first.

Every AWS-mutating script is dry-run by default, and the destructive ones require a typed
confirmation phrase. This is enforced by a test, not by convention
(`APPROVAL_GATES: AWS-mutating scripts default to dry-run`).

## Cost

Standing up the full platform costs real money — roughly $1.40/day for MSK plus EC2 while
running, and MSK cannot be stopped, only destroyed. Read [`docs/COST.md`](docs/COST.md)
before applying anything. Tier 0 of [`docs/DEMO.md`](docs/DEMO.md) runs the entire AI layer
and test suite offline for **$0**.

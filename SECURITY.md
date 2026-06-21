# Security Policy

## Reporting a Vulnerability

Please report security vulnerabilities **privately** — do not open a public
issue. Email **marvin@fam-feser.de** with:

- a description of the issue and its impact,
- steps to reproduce (proof-of-concept if possible),
- affected version(s)/commit.

You can expect an initial acknowledgement within a few days. Please allow
reasonable time for a fix to be prepared and released before any public
disclosure.

## Supported Versions

Security fixes target the latest released version on the default branch.

## Handling of Secrets

This project sources all credentials from environment variables or HashiCorp
Vault — never commit secrets, tokens, or private keys to the repository.

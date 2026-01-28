# Security Policy

## Supported Versions

Security updates are provided for the latest released version of **abx**.

If you are using an older version, please upgrade to the most recent version before reporting a security issue.

---

## Reporting a Vulnerability

If you believe you have found a security vulnerability in **abx**, please report it privately.

### Please include
- A clear description of the vulnerability
- Steps to reproduce (minimal example if possible)
- Impact assessment (what could happen if exploited)
- Your environment (OS, Python version, pandas version)
- Any logs or error messages

### Where to report
For now, please report security issues via a private channel appropriate for your repository setup:
- If this is hosted on GitHub: use **GitHub Security Advisories** (preferred), or a private message to maintainers.
- If you maintain a dedicated security email: send details there.

> Do **not** open a public GitHub issue for security vulnerabilities.

---

## Disclosure Process

After receiving a report, maintainers will:
1. Confirm receipt (as quickly as possible)
2. Investigate and reproduce the issue
3. Develop a fix and regression test
4. Publish a patched release
5. Credit the reporter if they want attribution

---

## Security Considerations (CLI tools)

`abx` is a local CLI tool that processes user-provided datasets.
Common classes of risk for data tools include:
- unsafe file path handling,
- unexpected code execution from untrusted inputs (should not happen in `abx`),
- denial-of-service via extremely large inputs,
- dependency vulnerabilities (e.g., pandas/pyarrow).

If you suspect any of the above, please report privately using the process above.
